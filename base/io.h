#ifndef BASE_IO_H_
#define BASE_IO_H_ 1

namespace ev {

bool PathIsRotational(int fd);

bool PathIsRotational(const char* path);

}  // namespace ev

#endif  // !BASE_IO_H_
