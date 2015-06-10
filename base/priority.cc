#include "base/priority.h"

#include <climits>

#include <sched.h>
#include <unistd.h>

#include <kj/debug.h>

#include "base/file.h"

namespace ev {

void SetPriorityLevel(PriorityLevel level) {
  switch (level) {
    case kPriorityLowest: {
      // Make sure we will be among the first to be killed by the OOM killer.
      auto oom_adj = ev::OpenFileStream("/proc/self/oom_score_adj", "w");
      if (oom_adj) {
        fputs("1000", oom_adj.get());
        oom_adj.reset();
      }

      // We only want to run on hardware threads that would otherwise be idle.
      sched_param sp;
      memset(&sp, 0, sizeof(sp));
      sched_setscheduler(0, SCHED_IDLE, &sp);

      // Also become a bit nicer, mostly to get a differently colored graph in
      // Munin.  We're not using INT_MAX, since this can overflow in glibc's
      // implementation if the previous nice value was not zero.
      nice(1);
    } break;

    default:
      KJ_FAIL_REQUIRE("invalid priority level");
  }
}

}  // namespace ev
