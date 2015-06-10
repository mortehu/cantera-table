#ifndef BASE_PRIORITY_H_
#define BASE_PRIORITY_H_ 1

namespace ev {

enum PriorityLevel {
  kPriorityLowest,
};

void SetPriorityLevel(PriorityLevel level);

}  // namespace

#endif  // !BASE_PRIORITY_H_
