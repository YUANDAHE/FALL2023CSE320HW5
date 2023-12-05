
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "csapp.h"
#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "protocol.h"

CLIENT_REGISTRY *cregistry;

static void xacto_send_replay_pkt(int fd, uint8_t status, uint32_t serial)
{
    XACTO_PACKET reply;
    struct timespec time;

    memset(&reply, 0x00, sizeof(XACTO_PACKET));
    reply.type = XACTO_REPLY_PKT;
    reply.status = status;
    reply.size = 0;
    reply.serial = serial;
    reply.null = 1;
    clock_gettime(CLOCK_MONOTONIC, &time);
    reply.timestamp_sec = time.tv_sec;
    reply.timestamp_nsec = time.tv_nsec;

    proto_send_packet(fd, &reply, NULL);
    
    return;
}

static void send_replay_ERROR(int fd, uint32_t serial)
{
    XACTO_PACKET reply;
    struct timespec time;

    memset(&reply, 0x00, sizeof(XACTO_PACKET));
    reply.type = XACTO_REPLY_PKT;
    reply.status = -1;
    reply.size = 0;
    reply.serial = serial;
    reply.null = 1;
    clock_gettime(CLOCK_MONOTONIC, &time);
    reply.timestamp_sec = time.tv_sec;
    reply.timestamp_nsec = time.tv_nsec;

    proto_send_packet(fd, &reply, NULL);
    
    return;
}

static void send_replay_OK(int fd, uint32_t serial)
{
    XACTO_PACKET reply;
    struct timespec time;

    memset(&reply, 0x00, sizeof(XACTO_PACKET));
    reply.type = XACTO_REPLY_PKT;
    reply.status = 0;
    reply.size = 0;
    reply.serial = serial;
    reply.null = 1;
    clock_gettime(CLOCK_MONOTONIC, &time);
    reply.timestamp_sec = time.tv_sec;
    reply.timestamp_nsec = time.tv_nsec;

    proto_send_packet(fd, &reply, NULL);
    
    return;
}

static void do_put_event(int fd, TRANSACTION *trans, XACTO_PACKET *pkt)
{
    int ret;
    BLOB *BLOB_KEY, *BLOB_VALUE;
    KEY *key;
    TRANS_STATUS trans_status;
    void *data = NULL;
    uint32_t serial_num;

    //XACTO_KEY_PKT
    ret = proto_recv_packet(fd, pkt, &data);
    if (ret == -1) {
        serial_num = ntohl(pkt->serial);
        send_replay_ERROR(fd, serial_num);
        return;
    }
    BLOB_KEY = blob_create(data, ntohl(pkt->size));
    key = key_create(BLOB_KEY);
    free(data);

    //XACTO_VALUE_PKT
    ret = proto_recv_packet(fd, pkt, &data);
    if (ret == -1) {
        serial_num = ntohl(pkt->serial);
        send_replay_ERROR(fd, serial_num);
        return;
    }
    BLOB_VALUE = blob_create(data, ntohl(pkt->size));
    free(data);

    trans_status = store_put(trans, key, BLOB_VALUE);
    serial_num = ntohl(pkt->serial);
    if (trans_status == TRANS_ABORTED) {
        send_replay_ERROR(fd, serial_num);
        return;
    }
    
    send_replay_OK(fd, serial_num);

    return;
}

static void do_get_event(int fd, TRANSACTION *trans, XACTO_PACKET *pkt)
{
    int ret;
    BLOB *BLOB_KEY, *BLOB_VALUE;
    KEY *key;
    TRANS_STATUS trans_status;
    void *data = NULL;
    uint32_t serial_num;

    //XACTO_KEY_PKT
    ret = proto_recv_packet(fd, pkt, &data);
    serial_num = ntohl(pkt->serial);
    if (ret == -1) {
        send_replay_ERROR(fd, serial_num);
        return;
    }
    BLOB_KEY = blob_create(data, ntohl(pkt->size));
    key = key_create(BLOB_KEY);
    free(data);

    BLOB_VALUE = NULL;
    trans_status = store_get(trans, key, &BLOB_VALUE);
    if (trans_status == TRANS_ABORTED){
        send_replay_ERROR(fd, serial_num);
        return;
    }
    
    send_replay_OK(fd, serial_num);
    
    //send value
    XACTO_PACKET value_pkt;
    struct timespec time;
    memset(&value_pkt, 0x00, sizeof(XACTO_PACKET));
    value_pkt.type = XACTO_VALUE_PKT;
    value_pkt.status = 0;
    value_pkt.size = BLOB_VALUE->size;
    value_pkt.serial = serial_num;
    value_pkt.null = 0;
    clock_gettime(CLOCK_MONOTONIC, &time);
    value_pkt.timestamp_sec = time.tv_sec;
    value_pkt.timestamp_nsec = time.tv_nsec;
    proto_send_packet(fd, &value_pkt, BLOB_VALUE->content);

    return;
}

void *xacto_client_service(void *client_arg)
{
    int ret;
    int client_fd = (int)(unsigned long)client_arg;
    TRANSACTION *trans;
    XACTO_PACKET xacto_packet;
    TRANS_STATUS trans_status;
    
    Pthread_detach(Pthread_self());

    trans = trans_create();
    if (trans == NULL) {
        creg_unregister(cregistry, client_fd);
        close(client_fd);
        printf("pthread %ld do fd %d end.\n", Pthread_self(), client_fd);
        return NULL;
    }

    while (1) {
        ret = proto_recv_packet(client_fd, &xacto_packet, NULL);
        if (ret == -1) {
            send_replay_ERROR(client_fd, 0);
            break;
        }

        switch (xacto_packet.type) {
        case XACTO_PUT_PKT:
            do_put_event(client_fd, trans, &xacto_packet);
            break;

        case XACTO_GET_PKT:
            do_get_event(client_fd, trans, &xacto_packet);
            break;

        case XACTO_COMMIT_PKT:
            trans_status = trans_commit(trans);
            if (trans_status == TRANS_ABORTED){
                fprintf(stderr, "trans_commit TRANS_ABORTED\n");
                send_replay_ERROR(client_fd, ntohl(xacto_packet.serial));
                continue;
            }
            send_replay_OK(client_fd, ntohl(xacto_packet.serial));
            break;

        default:
            fprintf(stderr, "type %d error\n", xacto_packet.type);
            send_replay_ERROR(client_fd, ntohl(xacto_packet.serial));
            break;
        }
    }

    creg_unregister(cregistry, client_fd);
    close(client_fd);
    
    printf("pthread %ld do fd %d end.\n", Pthread_self(), client_fd);

    return NULL;
}

