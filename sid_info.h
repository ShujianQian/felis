//
// Created by Shujian Qian on 2022-03-15.
//

#ifndef FELIS__SID_INFO_H_
#define FELIS__SID_INFO_H_

#include <cstdint>
#include <string>

namespace felis
{

inline std::string sid_info(uint64_t sid)
{
    return "{epoch: " + std::to_string(sid >> 32) + ", node_id: " + std::to_string(sid & 0xFF) + ", seq no:"
            + std::to_string(sid >> 8 & 0xFFFFFF) + "}";
}

}

#endif //FELIS__SID_INFO_H_
