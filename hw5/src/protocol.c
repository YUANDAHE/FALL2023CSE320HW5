
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "csapp.h"
#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "protocol.h"

int proto_send_packet(int fd, XACTO_PACKET *pkt, void *data)
{
    uint32_t data_len = pkt->size;

    if (pkt == NULL) {
        return -1;
    }
    
    pkt->serial         = htonl(pkt->serial);
    pkt->size           = htonl(pkt->size);
    pkt->timestamp_sec  = htonl(pkt->timestamp_sec);
    pkt->timestamp_nsec = htonl(pkt->timestamp_nsec);

    ssize_t writen = rio_writen(fd, pkt, sizeof(XACTO_PACKET));
    if (writen <= 0) 
    {
        return -1;
    }

    if (data && data_len > 0) 
    {
        writen = rio_writen(fd, data, data_len);
        if (writen <= 0) 
        {
            return -1;
        }
    }

    return 0;
}

int proto_recv_packet(int fd, XACTO_PACKET *pkt, void **datap)
{
    void *pkt_data;
    
	ssize_t readn = rio_readn(fd, pkt, sizeof(XACTO_PACKET));
	if (readn <= 0) 
    {
        return -1;
	}

    pkt->null = 1;
    uint32_t data_len = ntohl(pkt->size);
    if (data_len > 0) {
        if (datap == NULL) {
            return -1;
        }
        
        pkt_data = malloc(data_len + 1);
        memset(pkt_data, 0x00, data_len + 1);
        readn = rio_readn(fd, pkt_data, data_len);
        if (readn <= 0) 
        {
            free(pkt_data);
            return -1;
        }
        *datap = pkt_data;
        pkt->null = 0;
    }
    
    return 0;
}

