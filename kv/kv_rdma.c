#include "kv_rdma.h"

#include <fcntl.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "memery_operation.h"
#include "spdk/env.h"
#include "spdk/thread.h"

#define HEADER_SIZE (sizeof(uint64_t))
#define TIMEOUT_IN_MS (500U)
#define MAX_BUF_NUM 64
const int BUFFER_SIZE = 1024;

#define TEST_NZ(x)                                      \
    do {                                                \
        if ((x)) {                                      \
            fprintf(stderr, "error: " #x " failed.\n"); \
            return -1;                                  \
        }                                               \
    } while (0)
#define TEST_Z(x) TEST_NZ(!(x))

struct rdma_connection {
    struct kv_rdma *self;
    struct rdma_cm_id *cm_id;
    struct ibv_qp *qp;
    union {
        kv_rdma_connect_cb func;
        kv_rdma_req_handler handler;
    } cb;
    void *cb_arg;
    // server connection data
    uint32_t con_req_num;
    uint32_t max_msg_sz;
    struct ibv_mr *mr;
    struct request_ctx *requests;
};

struct kv_rdma {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct rdma_event_channel *ec;
    bool is_server;
};

struct request_ctx {
    struct rdma_connection *conn;
    kv_rdma_req_cb cb;
    void *cb_arg;
    struct ibv_mr *req, *resp;

    // server data
    // uint64_t resp_addr;
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

static int cq_poller(void *arg);
static int create_connetion(struct kv_rdma *self, struct rdma_cm_id *cm_id) {
    // --- build context ---
    if (self->ctx == NULL) {
        self->ctx = cm_id->verbs;
        TEST_Z(self->pd = ibv_alloc_pd(self->ctx));
        TEST_Z(self->cq = ibv_create_cq(self->ctx, 3 * MAX_BUF_NUM /* max_conn_num */, NULL, NULL, 0));
        TEST_NZ(ibv_req_notify_cq(self->cq, 0));  //?
        spdk_poller_register(cq_poller, self, 0);
    }
    // assume only have one context
    assert(self->ctx == cm_id->verbs);

    // --- build qp ---
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_attr.send_cq = self->cq;
    qp_attr.recv_cq = self->cq;
    qp_attr.qp_type = IBV_QPT_RC;

    qp_attr.cap.max_send_wr = MAX_BUF_NUM;
    qp_attr.cap.max_recv_wr = MAX_BUF_NUM;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    TEST_NZ(rdma_create_qp(cm_id, self->pd, &qp_attr));

    struct rdma_connection *conn = cm_id->context;
    conn->qp = cm_id->qp;

    return 0;
}

void kv_rdma_setup_conn_ctx(connection_handle h, uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler,
                            void *arg) {
    struct rdma_connection *conn = h;
    conn->cb.handler = handler;
    conn->cb_arg = arg;
    conn->con_req_num = con_req_num;
    conn->max_msg_sz = max_msg_sz;
    assert(conn->mr == NULL);
    uint8_t *buf = spdk_dma_malloc(con_req_num * max_msg_sz, 4, NULL);
    conn->mr = ibv_reg_mr(conn->self->pd, buf, con_req_num * max_msg_sz, IBV_ACCESS_LOCAL_WRITE);
    conn->requests = kv_calloc(con_req_num, sizeof(struct request_ctx));

    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge = {(uint64_t)buf, max_msg_sz, conn->mr->lkey};
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    for (size_t i = 0; i < con_req_num; i++) {
        conn->requests[i].conn = conn;
        conn->requests[i].buf = (uint8_t *)sge.addr;
        wr.wr_id = (uint64_t)(conn->requests + i);
        ibv_post_recv(conn->qp, &wr, &bad_wr);
        sge.addr += max_msg_sz;
    }
}

static int on_connect_request(struct kv_rdma *self, struct rdma_cm_event *event) {
    printf("received connection request.\n");
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection)), *lconn = event->id->context;
    *conn = (struct rdma_connection){self, event->id, NULL, .cb.func = NULL, .cb_arg = NULL};
    event->id->context = conn;
    TEST_NZ(create_connetion(self, event->id));
    // register_memory & post_receives
    if (lconn->cb.func) lconn->cb.func(conn, lconn->cb_arg);
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_accept(event->id, &cm_params));
    return 0;
}

static int on_addr_resolved(struct kv_rdma *self, struct rdma_cm_event *event) {
    printf("address resolved.\n");
    TEST_NZ(create_connetion(self, event->id));
    TEST_NZ(rdma_resolve_route(event->id, TIMEOUT_IN_MS));
    return 0;
}

static int on_route_resolved(struct rdma_cm_id *id) {
    printf("route resolved.\n");
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_connect(id, &cm_params));
    return 0;
}

static int on_disconnect(struct rdma_cm_event *e) {
    printf("peer disconnected.\n");

    struct rdma_connection *conn = e->id->context;

    rdma_destroy_qp(e->id);
    rdma_destroy_id(e->id);
    kv_free(conn);
    return 0;
}

static int kv_rdma_cm_poller(void *_self) {
    struct rdma_cm_event *event = NULL;
    struct kv_rdma *self = _self;
    while (rdma_get_cm_event(self->ec, &event) == 0) {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);
        switch (event_copy.event) {
            case RDMA_CM_EVENT_ADDR_RESOLVED:
                assert(!self->is_server);
                TEST_NZ(on_addr_resolved(self, &event_copy));
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:
                assert(!self->is_server);
                TEST_NZ(on_route_resolved(event_copy.id));
                break;
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                assert(self->is_server);
                TEST_NZ(on_connect_request(self, &event_copy));
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                if (!self->is_server) {
                    struct rdma_connection *conn = event_copy.id->context;
                    if (conn->cb.func) conn->cb.func(conn, conn->cb_arg);
                }
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                on_disconnect(&event_copy);
                break;
            default:
                break;
        }
    }
    return 0;
}

int kv_rdma_listen(kv_rdma_handle h, char *addr_str, char *port_str, kv_rdma_connect_cb cb, void *cb_arg) {
    struct kv_rdma *self = h;
    self->is_server = true;
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection));
    *conn = (struct rdma_connection){self, NULL, NULL, .cb.func = cb, .cb_arg = cb_arg};
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

int kv_rdma_connect(kv_rdma_handle h, char *addr_str, char *port_str, kv_rdma_connect_cb cb, void *cb_arg) {
    struct kv_rdma *self = h;
    self->is_server = false;
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection));
    *conn = (struct rdma_connection){self, NULL, NULL, .cb.func = cb, .cb_arg = cb_arg};
    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
    TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
    conn->cm_id->context = conn;
    freeaddrinfo(addr);
    return 0;
}

int kv_rdma_disconnect(connection_handle h) { return rdma_disconnect(((struct rdma_connection *)h)->cm_id); }

void kv_rmda_send_req(connection_handle h, kv_rmda_mr req, uint32_t req_sz, kv_rmda_mr resp, uint32_t resp_sz,
                      kv_rdma_req_cb cb, void *cb_arg) {
    struct rdma_connection *conn = h;
    assert(conn->self->is_server == false);
    struct request_ctx *ctx = kv_malloc(sizeof(struct request_ctx));
    *ctx = (struct request_ctx){conn, cb, cb_arg, req, resp};

    *(uint64_t *)ctx->req->addr = (uint64_t)ctx->resp->addr;

    struct ibv_sge sge[2] = {{(uintptr_t)ctx->req->addr, req_sz, ctx->req->lkey},
                             {(uintptr_t)ctx->resp->addr, resp_sz, ctx->resp->lkey}};
    struct ibv_send_wr s_wr, *s_bad_wr = NULL;
    memset(&s_wr, 0, sizeof(s_wr));
    s_wr.wr_id = (uintptr_t)ctx;
    s_wr.opcode = IBV_WR_SEND_WITH_IMM;
    s_wr.imm_data = ctx->resp->rkey;
    s_wr.sg_list = sge;
    s_wr.num_sge = 1;
    s_wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(conn->qp, &s_wr, &s_bad_wr)) {
        goto fail;
    }
    struct ibv_recv_wr r_wr = {(uintptr_t)ctx, NULL, sge + 1, 1}, *r_bad_wr = NULL;
    if (ibv_post_recv(conn->qp, &r_wr, &r_bad_wr)) {
        goto fail;
    }
    return;
fail:
    if (ctx->cb) ctx->cb(h, false, req, resp, ctx->cb_arg);
    kv_free(ctx);
}

static inline void on_write_resp_done(struct ibv_wc *wc) {
    struct request_ctx *ctx = (struct request_ctx *)wc->wr_id;
    assert(ctx->conn->self->is_server);
    struct ibv_sge sge = {(uint64_t)ctx->buf, ctx->conn->max_msg_sz, ctx->conn->mr->lkey};
    struct ibv_recv_wr wr = {(uint64_t)ctx, NULL, &sge, 1}, *bad_wr = NULL;
    ibv_post_recv(ctx->conn->qp, &wr, &bad_wr);
}

void kv_rdma_make_resp(void *req, uint8_t *resp, uint32_t resp_sz) {
    struct request_ctx *ctx = req;
    struct ibv_sge sge = {(uintptr_t)resp, resp_sz, ctx->conn->mr->lkey};
    struct ibv_send_wr wr, *bad_wr = NULL;
    wr.wr_id = (uintptr_t)ctx;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = *(uint64_t *)ctx->buf;
    wr.wr.rdma.rkey = ctx->resp_rkey;
    ibv_post_send(ctx->conn->qp, &wr, &bad_wr);
}

static inline void on_recv_req(struct ibv_wc *wc) {
    struct request_ctx *ctx = (struct request_ctx *)wc->wr_id;
    assert(ctx->conn->self->is_server);
    assert(wc->byte_len > HEADER_SIZE);
    assert(wc->wc_flags & IBV_WC_WITH_IMM);
    ctx->resp_rkey = wc->imm_data;
    ctx->conn->cb.handler(ctx, ctx->buf + HEADER_SIZE, wc->byte_len - HEADER_SIZE, ctx->conn->cb_arg);
}

#define MAX_ENTRIES_PER_POLL 32
static int cq_poller(void *arg) {
    struct kv_rdma *self = arg;
    struct ibv_wc wc[MAX_ENTRIES_PER_POLL];
    while (true) {
        int rc = ibv_poll_cq(self->cq, MAX_ENTRIES_PER_POLL, wc);
        if (rc <= 0) return rc;
        for (int i = 0; i < rc; i++) {
            // struct request_ctx *ctx = (struct request_ctx *)wc[i].wr_id;

            if (wc[i].status != IBV_WC_SUCCESS) {
                puts("cq_poller: status is not IBV_WC_SUCCESS.");
                exit(-1);
            }
            if (wc[i].opcode & IBV_WC_RECV || wc[i].opcode & IBV_WC_RECV_RDMA_WITH_IMM) {
                // printf("received message: %s\n", conn->recv_region);
                on_recv_req(wc + i);

            } else if (wc[i].opcode & IBV_WC_SEND) {
                printf("send completed successfully.\n");
            } else if (wc[i].opcode & IBV_WC_RDMA_WRITE) {
                on_write_resp_done(wc + i);
            }
        }
    }
}

kv_rdma_handle kv_rdma_alloc(void) {
    struct kv_rdma *self = kv_malloc(sizeof(struct kv_rdma));
    kv_memset(self, 0, sizeof(struct kv_rdma));
    self->ec = rdma_create_event_channel();
    if (!self->ec) {
        fprintf(stderr, "fail to create event channel.\n");
        return NULL;
    }
    int flag = fcntl(self->ec->fd, F_GETFL);
    fcntl(self->ec->fd, F_SETFL, flag | O_NONBLOCK);
    spdk_poller_register(kv_rdma_cm_poller, self, 1);
    return self;
}
void kv_rdma_free(kv_rdma_handle self) { kv_free(self); }