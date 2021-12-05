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

const int BUFFER_SIZE = 1024;

#define TEST_NZ(x)                                      \
    do {                                                \
        if ((x)) {                                      \
            fprintf(stderr, "error: " #x " failed.\n"); \
            return -1;                                  \
        }                                               \
    } while (0)
#define TEST_Z(x) TEST_NZ(!(x))

struct context {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    // struct ibv_comp_channel *comp_channel;
};
struct connection {
    struct ibv_qp *qp;
    struct ibv_pd *pd;
    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;

    char *recv_region;
    char *send_region;
};

struct kv_rdma {
    struct rdma_cm_id *listener;
    struct rdma_event_channel *ec;
    struct context *ctx;
};
void *kv_rdma_alloc(void) {
    struct kv_rdma *self = kv_malloc(sizeof(struct kv_rdma));
    kv_memset(self, 0, sizeof(struct kv_rdma));
    return self;
}
void kv_rdma_free(void *self) { kv_free(self); }

#define MAX_ENTRIES_PER_POLL 32
static int cq_poller(void *arg) {
    struct context *ctx = arg;
    struct ibv_wc wc[MAX_ENTRIES_PER_POLL];
    while (true) {
        int rc = ibv_poll_cq(ctx->cq, MAX_ENTRIES_PER_POLL, wc);
        if (rc <= 0) return rc;
        for (int i = 0; i < rc; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) puts("on_completion: status is not IBV_WC_SUCCESS.");

            if (wc[i].opcode & IBV_WC_RECV) {
                struct connection *conn = (struct connection *)(uintptr_t)wc[i].wr_id;

                printf("received message: %s\n", conn->recv_region);

            } else if (wc[i].opcode == IBV_WC_SEND) {
                printf("send completed successfully.\n");
            }
        }
    }
}

static int post_receives(struct connection *conn) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t)conn;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_region;
    sge.length = BUFFER_SIZE;
    sge.lkey = conn->recv_mr->lkey;

    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
    return 0;
}

static int register_memory(struct connection *conn) {
    conn->send_region = malloc(BUFFER_SIZE);
    conn->recv_region = spdk_dma_malloc(BUFFER_SIZE, 0, NULL);  // malloc(BUFFER_SIZE);
    TEST_Z(conn->send_mr = ibv_reg_mr(conn->pd, conn->send_region, BUFFER_SIZE, 0));
    TEST_Z(conn->recv_mr = ibv_reg_mr(conn->pd, conn->recv_region, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE));
    return 0;
}

static int on_connect_request(struct kv_rdma *self, struct rdma_cm_event *event) {
    printf("received connection request.\n");
    // --- build context ---
    if (self->ctx == NULL) {
        self->ctx = (struct context *)malloc(sizeof(struct context));
        self->ctx->ctx = event->id->verbs;
        TEST_Z(self->ctx->pd = ibv_alloc_pd(self->ctx->ctx));
        TEST_Z(self->ctx->cq = ibv_create_cq(self->ctx->ctx, 10, NULL, NULL, 0)); /* cqe=10 is arbitrary */
        TEST_NZ(ibv_req_notify_cq(self->ctx->cq, 0));
        spdk_poller_register(cq_poller, self->ctx, 0);
    }
    // assume only have one context
    assert(self->ctx->ctx == event->id->verbs);
    // --- build qp ---
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_attr.send_cq = self->ctx->cq;
    qp_attr.recv_cq = self->ctx->cq;
    qp_attr.qp_type = IBV_QPT_RC;

    qp_attr.cap.max_send_wr = 10;
    qp_attr.cap.max_recv_wr = 10;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    TEST_NZ(rdma_create_qp(event->id, self->ctx->pd, &qp_attr));

    struct rdma_conn_param cm_params;
    struct connection *conn;
    event->id->context = conn = (struct connection *)malloc(sizeof(struct connection));
    conn->qp = event->id->qp;
    conn->pd = self->ctx->pd;

    TEST_NZ(register_memory(conn));
    TEST_NZ(post_receives(conn));

    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_accept(event->id, &cm_params));

    return 0;
}
static int on_connection(void *context) {
    struct connection *conn = (struct connection *)context;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    snprintf(conn->send_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());

    printf("connected. posting send...\n");

    memset(&wr, 0, sizeof(wr));

    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)conn->send_region;
    sge.length = BUFFER_SIZE;
    sge.lkey = conn->send_mr->lkey;

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

    return 0;
}

static int on_disconnect(struct rdma_cm_id *id) {
    struct connection *conn = (struct connection *)id->context;

    printf("peer disconnected.\n");

    rdma_destroy_qp(id);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);

    free(conn->send_region);
    spdk_dma_free(conn->recv_region);

    free(conn);

    rdma_destroy_id(id);

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
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                TEST_NZ(on_connect_request(self, &event_copy));
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                on_connection(event_copy.id->context);
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                on_disconnect(event_copy.id);
                break;
            default:
                break;
        }
    }
    return 0;
}

int kv_rdma_listen(void *_self, char *addr_str, char *port_str) {
    struct kv_rdma *self = _self;
    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(addr_str, port_str, NULL, &addr));
    TEST_Z(self->ec = rdma_create_event_channel());
    int flag = fcntl(self->ec->fd, F_GETFL);
    TEST_NZ(fcntl(self->ec->fd, F_SETFL, flag | O_NONBLOCK));
    TEST_NZ(rdma_create_id(self->ec, &self->listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(self->listener, addr->ai_addr));
    TEST_NZ(rdma_listen(self->listener, 10)); /* backlog=10 is arbitrary */
    freeaddrinfo(addr);
    self->ctx = NULL;
    printf("kv rdma listening on %s %s.\n", addr_str, port_str);
    spdk_poller_register(kv_rdma_cm_poller, self, 1);
    return 0;
}