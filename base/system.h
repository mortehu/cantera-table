#ifndef BASE_SYSTEM_H_
#define BASE_SYSTEM_H_ 1

#include <string>

namespace ev {

std::string HostName();

std::string ReadLink(const char* path);

}  // namespace ev

#endif  // !BASE_SYSTEM_H_
