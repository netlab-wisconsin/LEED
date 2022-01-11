#include "kv_rdma.h"

#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <unistd.h>

#include "kv_app.h"
#include "memery_operation.h"
#include "spdk/env.h"
#include "uthash.h"

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
    // sever connection data
    UT_hash_handle hh;
    // client connection data
    pthread_mutex_t lock;
    STAILQ_HEAD(, client_req_ctx) request_ctxs;
};
struct cq_poller_ctx {
    struct kv_rdma *self;
    struct ibv_cq *cq;
    void *poller;
};
struct fini_ctx_t {
    uint32_t thread_id, io_cnt;
    kv_app_func cb;
    void *cb_arg;
};
struct kv_rdma {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct rdma_event_channel *ec;
    void *cm_poller;
    bool is_server;
    uint32_t thread_num, thread_id;
    struct cq_poller_ctx *cq_pollers;
    // server data
    struct ibv_srq *srq;
    uint32_t con_req_num;
    uint32_t max_msg_sz;
    struct ibv_mr *mr;
    pthread_rwlock_t lock;
    struct server_req_ctx *requests;
    struct rdma_connection *connections;
    // finish ctx
    struct fini_ctx_t fini_ctx;
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
    struct kv_rdma *self;
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
        TEST_Z(self->pd = ibv_alloc_pd(self->ctx));
        TEST_Z(self->cq = ibv_create_cq(self->ctx, 3 * MAX_Q_NUM /* max_conn_num */, NULL, NULL, 0));
        self->cq_pollers = kv_calloc(self->thread_num, sizeof(struct cq_poller_ctx));
        for (size_t i = 0; i < self->thread_num; i++) {
            self->cq_pollers[i] = (struct cq_poller_ctx){self, self->cq};
            kv_app_poller_register_on(self->thread_id + i, rdma_cq_poller, self->cq_pollers + i, 0,
                                      &self->cq_pollers[i].poller);
        }
        self->srq = NULL;

        if (self->is_server) {
            struct ibv_srq_init_attr srq_init_attr;
            memset(&srq_init_attr, 0, sizeof(srq_init_attr));
            srq_init_attr.attr.max_wr = MAX_Q_NUM;
            srq_init_attr.attr.max_sge = 1;
            TEST_Z(self->srq = ibv_create_srq(self->pd, &srq_init_attr));
            assert(self->mr == NULL);
            uint8_t *buf = spdk_dma_malloc(self->con_req_num * self->max_msg_sz, 4, NULL);
            self->mr = ibv_reg_mr(self->pd, buf, self->con_req_num * self->max_msg_sz, IBV_ACCESS_LOCAL_WRITE);
            self->requests = kv_calloc(self->con_req_num, sizeof(struct server_req_ctx));

            struct ibv_recv_wr wr, *bad_wr = NULL;
            struct ibv_sge sge = {(uint64_t)buf, self->max_msg_sz, self->mr->lkey};
            wr.next = NULL;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            for (size_t i = 0; i < self->con_req_num; i++) {
                self->requests[i].self = self;
                self->requests[i].buf = (uint8_t *)sge.addr;
                wr.wr_id = (uint64_t)(self->requests + i);
                TEST_NZ(ibv_post_srq_recv(self->srq, &wr, &bad_wr));
                sge.addr += self->max_msg_sz;
            }
        }
    }
    // assume only have one context
    assert(self->ctx == cm_id->verbs);

    // --- build qp ---
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_attr.send_cq = self->cq;
    qp_attr.recv_cq = self->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.srq = self->srq;

    qp_attr.cap.max_send_wr = MAX_Q_NUM;
    qp_attr.cap.max_recv_wr = MAX_Q_NUM;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    TEST_NZ(rdma_create_qp(cm_id, self->pd, &qp_attr));

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
    *conn = (struct rdma_connection){self, cm_id, NULL, .cb.handler = lconn->cb.handler, .cb_arg = lconn->cb_arg};
    cm_id->context = conn;
    TEST_NZ(create_connetion(self, cm_id));
    pthread_rwlock_wrlock(&self->lock);
    HASH_ADD_INT(self->connections, qp->qp_num, conn);
    pthread_rwlock_unlock(&self->lock);
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
    if (conn->self->is_server) {
        pthread_rwlock_wrlock(&conn->self->lock);
        HASH_DEL(conn->self->connections, conn);
        pthread_rwlock_unlock(&conn->self->lock);
    }
    rdma_destroy_qp(cm_id);
    rdma_destroy_id(cm_id);
    if (!conn->self->is_server && conn->cb.disconnect) conn->cb.disconnect(conn->cb_arg);
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
    pthread_mutex_init(&conn->lock, NULL);
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

    pthread_mutex_lock(&conn->lock);
    STAILQ_INSERT_TAIL(&conn->request_ctxs, ctx, next);
    pthread_mutex_unlock(&conn->lock);

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
int kv_rdma_listen(kv_rdma_handle h, char *addr_str, char *port_str, uint32_t con_req_num, uint32_t max_msg_sz,
                   kv_rdma_req_handler handler, void *arg) {
    struct kv_rdma *self = h;
    self->is_server = true;
    self->connections = NULL;
    struct rdma_connection *conn = kv_malloc(sizeof(struct rdma_connection));
    *conn = (struct rdma_connection){self, NULL, NULL, .cb.handler = handler, .cb_arg = arg};
    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
    TEST_NZ(rdma_create_id(self->ec, &conn->cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(conn->cm_id, addr->ai_addr));
    TEST_NZ(rdma_listen(conn->cm_id, 10)); /* backlog=10 is arbitrary  TODO:conn_num*/
    conn->cm_id->context = conn;
    freeaddrinfo(addr);
    self->con_req_num = con_req_num;
    self->max_msg_sz = max_msg_sz + HEADER_SIZE;
    pthread_rwlock_init(&self->lock, NULL);
    printf("kv rdma listening on %s %s.\n", addr_str, port_str);
    return 0;
}

void kv_rdma_make_resp(void *req, uint8_t *resp, uint32_t resp_sz) {
    struct server_req_ctx *ctx = req;
    struct ibv_sge sge = {(uintptr_t)resp, resp_sz, ctx->self->mr->lkey};
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
    assert(ctx->self->is_server);
    struct ibv_sge sge = {(uint64_t)ctx->buf, ctx->self->max_msg_sz, ctx->self->mr->lkey};
    struct ibv_recv_wr wr = {(uint64_t)ctx, NULL, &sge, 1}, *bad_wr = NULL;
    TEST_NZ(ibv_post_srq_recv(ctx->self->srq, &wr, &bad_wr));
}

static inline void on_recv_req(struct ibv_wc *wc) {
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "on_recv_req: status is %d\n", wc->status);
        wc->status = IBV_WC_SUCCESS;
        on_write_resp_done(wc);
        return;
    }
    struct server_req_ctx *ctx = (struct server_req_ctx *)wc->wr_id;
    assert(ctx->self->is_server);
    assert(wc->byte_len > HEADER_SIZE);
    assert(wc->wc_flags & IBV_WC_WITH_IMM);
    pthread_rwlock_rdlock(&ctx->self->lock);
    HASH_FIND_INT(ctx->self->connections, &wc->qp_num, ctx->conn);
    pthread_rwlock_unlock(&ctx->self->lock);
    assert(ctx->conn);
    ctx->resp_rkey = wc->imm_data;
    ctx->conn->cb.handler(ctx, ctx->buf + HEADER_SIZE, wc->byte_len - HEADER_SIZE, ctx->conn->cb_arg);
}

static inline void on_recv_resp(struct ibv_wc *wc) {
    struct rdma_connection *conn = (struct rdma_connection *)wc->wr_id;

    // using wc->imm_data(rkey) to find corresponding request_ctx
    pthread_mutex_lock(&conn->lock);
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
    pthread_mutex_unlock(&conn->lock);
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
    struct cq_poller_ctx *ctx = arg;
    struct ibv_wc wc[MAX_ENTRIES_PER_POLL];
    while (ctx->cq) {
        int rc = ibv_poll_cq(ctx->cq, MAX_ENTRIES_PER_POLL, wc);
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
void kv_rdma_init(kv_rdma_handle *h, uint32_t thread_num) {
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
    self->thread_num = thread_num;
    self->thread_id = kv_app_get_thread_index();
    *h = self;
}
static void poller_unregister_done(void *arg) {
    struct kv_rdma *self = arg;
    if (--self->fini_ctx.io_cnt) return;
    if (self->ctx) {
        ibv_destroy_cq(self->cq);
        ibv_dealloc_pd(self->pd);
        kv_free(self->cq_pollers);
        if (self->is_server) {
            ibv_destroy_srq(self->srq);
            kv_free(self->requests);
            kv_rdma_free_mr(self->mr);
        }
    }
    kv_app_send(self->fini_ctx.thread_id, self->fini_ctx.cb, self->fini_ctx.cb_arg);
    kv_free(self);
}

static void cq_poller_unregister(void *arg) {
    struct cq_poller_ctx *ctx = arg;
    ctx->cq = NULL;
    kv_app_poller_unregister(&ctx->poller);
    kv_app_send(ctx->self->thread_id, poller_unregister_done, ctx->self);
}
static void cm_poller_unregister(void *arg) {
    struct kv_rdma *self = arg;
    rdma_destroy_event_channel(self->ec);
    self->ec = NULL;
    kv_app_poller_unregister(&self->cm_poller);
    kv_app_send(self->thread_id, poller_unregister_done, self);
}
void kv_rdma_fini(kv_rdma_handle h, kv_rdma_fini_cb cb, void *cb_arg) {
    struct kv_rdma *self = h;
    self->fini_ctx = (struct fini_ctx_t){kv_app_get_thread_index(), 1, cb, cb_arg};
    kv_app_send(self->thread_id, cm_poller_unregister, self);
    if (self->ctx) {
        self->fini_ctx.io_cnt += self->thread_num;
        for (size_t i = 0; i < self->thread_num; i++)
            kv_app_send(self->thread_id + i, cq_poller_unregister, self->cq_pollers + i);
    }
}
