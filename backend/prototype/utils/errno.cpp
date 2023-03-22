#include "errno.hpp"

#include <cerrno>

const char* errnoStr() {
    switch (errno) {
        case EAGAIN:
            return "EAGAIN/EWOULDBLOCK";
        case EBADF:
            return "EBADF";
        case EDESTADDRREQ:
            return "EDESTADDRREQ";
        case EDQUOT:
            return "EDQUOT";
        case EFAULT:
            return "EFAULT";
        case EFBIG:
            return "EFBIG";
        case EINTR:
            return "EINTR";
        case EINVAL:
            return "EINVAL";
        case EIO:
            return "EIO";
        case ENOSPC:
            return "ENOSPC";
        case EPIPE:
            return "EPIPE";
        case EACCES:
            return "EACCES";
        default:
            return "NULL";
    }
}