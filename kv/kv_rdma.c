#include "kv_rdma.h"

#include <fcntl.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <unistd.h>

#include "kv_app.h"
#include "memery_operation.h"
#include "spdk/env.h"

#define HEADER_SIZE (sizeof(uint64_t))
#define TIMEOUT_IN_MS (500U)
#define MAX_Q_NUM (4096U)

#define TEST_NZ(x)                                      \
    do {                                                \
        if ((x)) {                                      \
            fprintf(stderr, "error: " #x " failed.\n"); \
            exit(-1);                                   \
        }                                               \
    } while (0)
#define TEST_Z(x) TEST_NZ(!(x))

struct rdma_connection {
    struct kv_rdma *self;
    struct rdma_cm_id *cm_id;
    struct ibv_qp *qp;
    union {
        kv_rdma_connect_cb connect;
        kv_rdma_req_handler handler;
        kv_rdma_disconnect_cb disconnect;
    } cb;
    void *cb_arg;
    // client connection data
    STAILQ_HEAD(, client_req_ctx) request_ctxs;
    // server connection data
    uint32_t con_req_num;
    uint32_t max_msg_sz;
    struct ibv_mr *mr;
    struct server_req_ctx *requests;
};

struct kv_rdma {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct rdma_event_channel *ec;
    void *cm_poller, *cq_poller;
    bool is_server;
};
struct client_req_ctx {
    struct rdma_connection *conn;
    kv_rdma_req_cb cb;
    void *cb_arg;
    struct ibv_mr *req, *resp;
    STAILQ_ENTRY(client_req_ctx) next;
};
struct server_req_ctx {
    struct rdma_connection *conn;
    uint32_t resp_rkey;
    uint8_t *buf;
};

// --- alloc and free ---
kv_rmda_mr kv_rdma_alloc_req(kv_rdma_handle h, uint32_t size) {
    struct kv_rdma *self = h;
    size += HEADER_SIZE;
    uint8_t *buf = spdk_dma_malloc(size, 4, NULL);
    return ibv_reg_mr(self->pd, buf, size, 0);
}

uint8_t *kv_rdma_get_req_buf(kv_rmda_mr mr) { return (uint8_t *)((struct ibv_mr *)mr)->addr + HEADER_SIZE; }

kv_rmda_mr kv_rdma_alloc_resp(kv_rdma_handle h, uint32_t size) {
    struct kv_rdma *self = h;
    uint8_t *buf = spdk_dma_malloc(size, 4, NULL);
    return ibv_reg_mr(self->pd, buf, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
}

uint8_t *kv_rdma_get_resp_buf(kv_rmda_mr mr) { return (uint8_t *)((struct ibv_mr *)mr)->addr; }

void kv_rdma_free_mr(kv_rmda_mr h) {
    struct ibv_mr *mr = h;
    uint8_t *buf = mr->addr;
    ibv_dereg_mr(mr);
    spdk_dma_free(buf);
}

// --- cm_poller ---
static int rdma_cq_poller(void *arg);
static int create_connetion(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    // --- build context ---
    if (self->ctx == NULL) {
        self->ctx = cm_id->verbs;
        self->pd = cm_id->pd;
        TEST_Z(self->cq = ibv_create_cq(self->ctx, 3 * MAX_Q_NUM /* max_conn_num */, NULL, NULL, 0));
        // TEST_NZ(ibv_req_notify_cq(self->cq, 0));  //?
        self->cq_poller = kv_app_poller_register(rdma_cq_poller, self, 0);
    }
    // assume only have one context
    assert(self->ctx == cm_id->verbs);

    // --- build qp ---
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_attr.send_cq = self->cq;
    qp_attr.recv_cq = self->cq;
    qp_attr.qp_type = IBV_QPT_RC;

    qp_attr.cap.max_send_wr = MAX_Q_NUM;
    qp_attr.cap.max_recv_wr = MAX_Q_NUM;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    TEST_NZ(rdma_create_qp(cm_id, cm_id->pd, &qp_attr));

    struct rdma_connection *conn = cm_id->context;
    conn->qp = cm_id->qp;

    return 0;
}

static inline int on_addr_resolved(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    assert(!self->is_server);
    printf("address resolved.\n");
    TEST_NZ(create_connetion(self, cm_id));
    TEST_NZ(rdma_resolve_route(cm_id, TIMEOUT_IN_MS));
    return 0;
}

static inline int on_route_resolved(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    assert(!self->is_server);
    printf("route resolved.\n");
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_connect(cm_id, &cm_params));
    return 0;
}

static inline int on_connect_request(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    assert(self->is_server);
    printf("received connection request.\n");
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection)), *lconn = cm_id->context;
    *conn = (struct rdma_connection){self, cm_id, NULL, .cb.connect = NULL, .cb_arg = NULL};
    cm_id->context = conn;
    TEST_NZ(create_connetion(self, cm_id));
    if (lconn->cb.connect) lconn->cb.connect(conn, lconn->cb_arg);
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_accept(cm_id, &cm_params));
    return 0;
}
static inline int on_connect_error(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    if (!self->is_server) {
        struct rdma_connection *conn = cm_id->context;
        if (conn->cb.connect) conn->cb.connect(NULL, conn->cb_arg);
        kv_free(conn);
    }
    return 0;
}
static inline int on_established(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    if (!self->is_server) {
        struct rdma_connection *conn = cm_id->context;
        if (conn->cb.connect) conn->cb.connect(conn, conn->cb_arg);
    }
    return 0;
}
static inline int on_disconnect(struct rdma_cm_id *cm_id) {
    printf("peer disconnected.\n");
    struct rdma_connection *conn = cm_id->context;
    rdma_destroy_qp(cm_id);
    rdma_destroy_id(cm_id);
    if (conn->self->is_server) {
        kv_free(conn->requests);
        kv_rdma_free_mr(conn->mr);
    } else if (conn->cb.disconnect)
        conn->cb.disconnect(conn->cb_arg);
    kv_free(conn);
    return 0;
}

static int rdma_cm_poller(void *_self) {
    struct kv_rdma *self = _self;
    struct rdma_cm_event *event = NULL;
    while (self->ec && rdma_get_cm_event(self->ec, &event) == 0) {
        struct rdma_cm_id *cm_id = event->id;
        enum rdma_cm_event_type event_type = event->event;
        rdma_ack_cm_event(event);
        switch (event_type) {
            case RDMA_CM_EVENT_ADDR_RESOLVED:
                on_addr_resolved(self, cm_id);
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:
                on_route_resolved(self, cm_id);
                break;
            case RDMA_CM_EVENT_UNREACHABLE:
            case RDMA_CM_EVENT_REJECTED:
                on_connect_error(self, cm_id);
                break;
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                on_connect_request(self, cm_id);
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                on_established(self, cm_id);
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                on_disconnect(cm_id);
                break;
            default:
                break;
        }
    }
    return 0;
}

// --- client ---

int kv_rdma_connect(kv_rdma_handle h, char *addr_str, char *port_str, kv_rdma_connect_cb cb, void *cb_arg) {
    struct kv_rdma *self = h;
    self->is_server = false;
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection));
    *conn = (struct rdma_connection){self, NULL, NULL, .cb.connect = cb, .cb_arg = cb_arg};
    STAILQ_INIT(&conn->request_ctxs);
    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
    TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
    conn->cm_id->context = conn;
    freeaddrinfo(addr);
    return 0;
}

void kv_rmda_send_req(connection_handle h, kv_rmda_mr req, uint32_t req_sz, kv_rmda_mr resp, kv_rdma_req_cb cb, void *cb_arg) {
    struct rdma_connection *conn = h;
    assert(conn->self->is_server == false);
    struct client_req_ctx *ctx = kv_malloc(sizeof(struct client_req_ctx));
    *ctx = (struct client_req_ctx){conn, cb, cb_arg, req, resp};

    *(uint64_t *)ctx->req->addr = (uint64_t)ctx->resp->addr;

    struct ibv_recv_wr r_wr = {(uintptr_t)conn, NULL, NULL, 0}, *r_bad_wr = NULL;
    if (ibv_post_recv(conn->qp, &r_wr, &r_bad_wr)) {
        goto fail;
    }
    struct ibv_sge sge = {(uintptr_t)ctx->req->addr, req_sz + HEADER_SIZE, ctx->req->lkey};
    struct ibv_send_wr s_wr, *s_bad_wr = NULL;
    memset(&s_wr, 0, sizeof(s_wr));
    s_wr.wr_id = (uintptr_t)ctx;
    s_wr.opcode = IBV_WR_SEND_WITH_IMM;
    s_wr.imm_data = ctx->resp->rkey;
    s_wr.sg_list = &sge;
    s_wr.num_sge = 1;
    s_wr.send_flags = IBV_SEND_SIGNALED;
    if (ibv_post_send(conn->qp, &s_wr, &s_bad_wr)) {
        goto fail;
    }
    STAILQ_INSERT_TAIL(&conn->request_ctxs, ctx, next);
    return;
fail:
    if (ctx->cb) ctx->cb(h, false, req, resp, ctx->cb_arg);
    kv_free(ctx);
}

void kv_rdma_disconnect(connection_handle h, kv_rdma_disconnect_cb cb, void *cb_arg) {
    struct rdma_connection *conn = h;
    conn->cb.disconnect = cb;
    conn->cb_arg = cb_arg;
    TEST_NZ(rdma_disconnect(conn->cm_id));
}

// --- server ---
int kv_rdma_listen(kv_rdma_handle h, char *addr_str, char *port_str, kv_rdma_connect_cb cb, void *cb_arg) {
    struct kv_rdma *self = h;
    self->is_server = true;
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection));
    *conn = (struct rdma_connection){self, NULL, NULL, .cb.connect = cb, .cb_arg = cb_arg};
    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
    TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(conn->cm_id, addr->ai_addr));
    TEST_NZ(rdma_listen(conn->cm_id, 10)); /* backlog=10 is arbitrary  TODO:conn_num*/
    conn->cm_id->context = conn;
    freeaddrinfo(addr);
    printf("kv rdma listening on %s %s.\n", addr_str, port_str);
    return 0;
}

void kv_rdma_setup_conn_ctx(connection_handle h, uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler,
                            void *arg) {
    struct rdma_connection *conn = h;
    conn->cb.handler = handler;
    conn->cb_arg = arg;
    conn->con_req_num = con_req_num;
    conn->max_msg_sz = max_msg_sz + HEADER_SIZE;
    assert(conn->mr == NULL);
    uint8_t *buf = spdk_dma_malloc(con_req_num * conn->max_msg_sz, 4, NULL);
    conn->mr = ibv_reg_mr(conn->cm_id->pd, buf, con_req_num * conn->max_msg_sz, IBV_ACCESS_LOCAL_WRITE);
    conn->requests = kv_calloc(con_req_num, sizeof(struct server_req_ctx));

    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge = {(uint64_t)buf, conn->max_msg_sz, conn->mr->lkey};
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    for (size_t i = 0; i < con_req_num; i++) {
        conn->requests[i].conn = conn;
        conn->requests[i].buf = (uint8_t *)sge.addr;
        wr.wr_id = (uint64_t)(conn->requests + i);
        TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
        sge.addr += conn->max_msg_sz;
    }
}

void kv_rdma_make_resp(void *req, uint8_t *resp, uint32_t resp_sz) {
    struct server_req_ctx *ctx = req;
    struct ibv_sge sge = {(uintptr_t)resp, resp_sz, ctx->conn->mr->lkey};
    struct ibv_send_wr wr, *bad_wr = NULL;
    wr.wr_id = (uintptr_t)ctx;
    wr.next = NULL;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.imm_data = ctx->resp_rkey;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = *(uint64_t *)ctx->buf;
    wr.wr.rdma.rkey = ctx->resp_rkey;
    TEST_NZ(ibv_post_send(ctx->conn->qp, &wr, &bad_wr));
}

// --- cq_poller ---
static inline void on_write_resp_done(struct ibv_wc *wc) {
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "on_write_resp_done: status is %d\n", wc->status);
    }
    struct server_req_ctx *ctx = (struct server_req_ctx *)wc->wr_id;
    assert(ctx->conn->self->is_server);
    struct ibv_sge sge = {(uint64_t)ctx->buf, ctx->conn->max_msg_sz, ctx->conn->mr->lkey};
    struct ibv_recv_wr wr = {(uint64_t)ctx, NULL, &sge, 1}, *bad_wr = NULL;
    TEST_NZ(ibv_post_recv(ctx->conn->qp, &wr, &bad_wr));
}

static inline void on_recv_req(struct ibv_wc *wc) {
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "on_recv_req: status is %d\n", wc->status);
        wc->status = IBV_WC_SUCCESS;
        on_write_resp_done(wc);
        return;
    }
    struct server_req_ctx *ctx = (struct server_req_ctx *)wc->wr_id;
    assert(ctx->conn->self->is_server);
    assert(wc->byte_len > HEADER_SIZE);
    assert(wc->wc_flags & IBV_WC_WITH_IMM);
    ctx->resp_rkey = wc->imm_data;
    ctx->conn->cb.handler(ctx, ctx->buf + HEADER_SIZE, wc->byte_len - HEADER_SIZE, ctx->conn->cb_arg);
}

static inline void on_recv_resp(struct ibv_wc *wc) {
    struct rdma_connection *conn = (struct rdma_connection *)wc->wr_id;

    // using wc->imm_data(rkey) to find corresponding request_ctx
    struct client_req_ctx *ctx = STAILQ_FIRST(&conn->request_ctxs), *tmp = ctx;
    assert(ctx);
    if (ctx->resp->rkey == wc->imm_data)  // likely
        STAILQ_REMOVE_HEAD(&conn->request_ctxs, next);
    else
        while (true) {
            TEST_Z((ctx = STAILQ_NEXT(tmp, next)));
            if (ctx->resp->rkey == wc->imm_data) {
                STAILQ_REMOVE_AFTER(&conn->request_ctxs, tmp, next);
                break;
            }
            tmp = ctx;
        }

    assert(!ctx->conn->self->is_server);
    assert(wc->wc_flags & IBV_WC_WITH_IMM);
    ctx->cb(ctx->conn, wc->status == IBV_WC_SUCCESS, ctx->req, ctx->resp, ctx->cb_arg);
}

static inline void on_send_req(struct ibv_wc *wc) {
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "on_send_req: status is %d\n", wc->status);
    }
}

#define MAX_ENTRIES_PER_POLL 128
static int rdma_cq_poller(void *arg) {
    struct kv_rdma *self = arg;
    struct ibv_wc wc[MAX_ENTRIES_PER_POLL];
    while (self->cq) {
        int rc = ibv_poll_cq(self->cq, MAX_ENTRIES_PER_POLL, wc);
        if (rc <= 0) return rc;
        for (int i = 0; i < rc; i++) {
            switch (wc[i].opcode) {
                case IBV_WC_RECV:
                    on_recv_req(wc + i);
                    break;
                case IBV_WC_RECV_RDMA_WITH_IMM:
                    on_recv_resp(wc + i);
                    break;
                case IBV_WC_RDMA_WRITE:
                    on_write_resp_done(wc + i);
                    break;
                case IBV_WC_SEND:
                    on_send_req(wc + i);
                    break;
                default:
                    fprintf(stderr, "kv_rdma: unknown event %u \n.", wc[i].opcode);
                    break;
            }
        }
    }
    return 0;
}

// --- init & fini ---
void kv_rdma_init(kv_rdma_handle *h) {
    struct kv_rdma *self = kv_malloc(sizeof(struct kv_rdma));
    kv_memset(self, 0, sizeof(struct kv_rdma));
    self->ec = rdma_create_event_channel();
    if (!self->ec) {
        fprintf(stderr, "fail to create event channel.\n");
        exit(-1);
    }
    int flag = fcntl(self->ec->fd, F_GETFL);
    fcntl(self->ec->fd, F_SETFL, flag | O_NONBLOCK);
    self->cm_poller = kv_app_poller_register(rdma_cm_poller, self, 1);
    *h = self;
}
void kv_rdma_fini(kv_rdma_handle h) {
    struct kv_rdma *self = h;
    rdma_destroy_event_channel(self->ec);
    self->ec = NULL;
    kv_app_poller_unregister(&self->cm_poller);
    if (self->ctx) {
        ibv_destroy_cq(self->cq);
        self->cq = NULL;
        kv_app_poller_unregister(&self->cq_poller);
    }
    kv_free(self);
}
