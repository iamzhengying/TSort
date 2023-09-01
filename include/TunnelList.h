#pragma once

typedef struct TunnelEntryLock {
    uint32_t floor;
    uint32_t ceiling;
    TreeNodeLock* root;
    TunnelEntryLock* next;
} TunnelEntryLock;