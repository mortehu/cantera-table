#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <limits>
#include <mutex>

#include "src/ca-table.h"
#include "src/keywords.h"
#include "src/query.h"
#include "src/util.h"

#include "third_party/evenk/evenk/synch_queue.h"
#include "third_party/evenk/evenk/thread_pool.h"

template <typename T>
using thread_pool_queue = evenk::synch_queue<T>;

namespace cantera {
namespace table {

using namespace internal;

namespace {

// Finds the first position of an array where the offset is not less than
// `offset'.
const struct ca_offset_score* CA_offset_score_lower_bound(
    const struct ca_offset_score* begin, const struct ca_offset_score* end,
    uint64_t offset) {
  const struct ca_offset_score* middle;
  size_t half, len = end - begin;

  while (len > 0) {
    half = len >> 1;
    middle = begin + half;

    if (middle->offset < offset) {
      begin = middle;
      ++begin;
      len = len - half - 1;
    } else
      len = half;
  }

  return begin;
}

std::string DayToDate(float day) {
  const auto day_tt = static_cast<time_t>(day * 86400);
  tm day_tm;
  gmtime_r(&day_tt, &day_tm);

  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", &day_tm);

  return std::string(buf);
}

// Processes a given score range for single index entry.
//
// Parameters:
//
//   key: The index entry key (e.g. "alexa:rank").
//   offsets_A: The offsets from the A query set.
//   offsets_B: The offsets from the B query set.
//   key_offsets: The offsets associated with the index entry.
//   limit_A: Minimum number of offsets that must match set A to achieve the
//            desired level of statistical significance.
//   limit_B: Minimum number of offsets that must match set B to achieve the
//            desired level of statistical significance.
//   prior_logit: The log-odds of the prior probability of an item belonging to
//                set A.
//   do_timestamps: Set to true if we're doing event prediction.
//   output_mutex: Mutex controlling access to stdout.
//   min_score: The lower bound of the range of score values to accept from
//              key_offsets.
//   max_score: The upper bound of the range of score values to accept from
//              key_offsets.
void ProcessRange(const string_view& key,
                  const std::vector<ca_offset_score>& offsets_A,
                  const std::vector<ca_offset_score>& offsets_B,
                  const std::vector<ca_offset_score>& key_offsets,
                  const size_t limit_A, const size_t limit_B,
                  const double prior_logit, const bool do_timestamps,
                  std::mutex& output_mutex, const float min_score = -HUGE_VAL,
                  const float max_score = HUGE_VAL) {
  const auto* A_begin = &offsets_A[0];
  const auto* B_begin = &offsets_B[0];
  const auto* K_begin = &key_offsets[0];

  const auto* A_end = &offsets_A[offsets_A.size()];
  const auto* B_end = &offsets_B[offsets_B.size()];
  const auto* K_end = &key_offsets[key_offsets.size()];

  size_t match_count_A = 0;
  size_t match_count_B = 0;
  size_t match_count_A_or_B = 0;

  for (auto K = K_begin, A = A_begin, B = B_begin; K != K_end;) {
    auto offset = K->offset;

    if (K->score < min_score || K->score > max_score) {
      ++K;
      continue;
    }

    A = CA_offset_score_lower_bound(A, A_end, offset);
    B = CA_offset_score_lower_bound(B, B_end, offset);

    bool match = false;

    if (A != A_end && A->offset == offset) {
      ++match_count_A;
      match = true;
    }

    if (B != B_end && B->offset == offset) {
      ++match_count_B;
      match = true;
    }

    if (match) ++match_count_A_or_B;

    // Skip duplicate offsets.
    do {
      ++K;
    } while (K != K_end && K->offset == offset);
  }

  if (match_count_A < limit_A && match_count_B < limit_B) return;

  const auto log_odds = std::log((match_count_A + 1.0) /
                                 (match_count_A_or_B - match_count_A + 1.0)) -
                        prior_logit;

  // Cutoff at >55% and <45%.
  if (std::fabs(log_odds) < std::log(.55 / (1.0 - .55))) return;

  std::unique_lock<std::mutex> lk(output_mutex);

  KJ_REQUIRE(match_count_A > 0 || match_count_B > 0, match_count_A,
             match_count_B);

  printf("%.3f\t%zu\t%zu\t%.*s", log_odds, match_count_A, match_count_B,
         static_cast<int>(key.size()), key.data());

  std::string min_score_string, max_score_string;
  if (!Keywords::GetInstance().IsTimestamped(key)) {
    min_score_string = FloatToString(min_score);
    max_score_string = FloatToString(max_score);
  } else {
    if (do_timestamps) {
      min_score_string = FloatToString(min_score) + " days ago";
      max_score_string = FloatToString(max_score) + " days ago";
    } else {
      if (std::isfinite(min_score)) min_score_string = DayToDate(min_score);
      if (std::isfinite(max_score)) max_score_string = DayToDate(max_score);
    }
  }

  // Print range operator, if applicable.
  if (std::isfinite(min_score)) {
    if (std::isfinite(max_score)) {
      printf("[%s,%s]", min_score_string.c_str(), max_score_string.c_str());
    } else {
      printf("≥%s", min_score_string.c_str());
    }
  } else if (std::isfinite(max_score)) {
    printf("≤%s", max_score_string.c_str());
  }

  putchar('\n');

  fflush(stdout);
}

// Processes a single index entry.
//
// Parameters:
//
//   key: The index entry key (e.g. "alexa:rank").
//   offsets_A: The offsets from the A query set.
//   offsets_B: The offsets from the B query set.
//   key_offsets: The offsets associated with the index entry.
//   limit_A: Minimum number of offsets that must match set A to achieve the
//            desired level of statistical significance.
//   limit_B: Minimum number of offsets that must match set B to achieve the
//            desired level of statistical significance.
//   prior_logit: The log-odds of the prior probability of an item belonging to
//                set A.
//   output_mutex: Mutex controlling access to stdout.
void ProcessSeries(std::string key, std::vector<ca_offset_score>&& key_offsets,
                   const std::vector<ca_offset_score>& offsets_A,
                   const std::vector<ca_offset_score>& offsets_B,
                   const size_t limit_A, const size_t limit_B,
                   const double prior_logit, const bool do_timestamps,
                   std::mutex& output_mutex) {
  if ((offsets_A.back().offset < key_offsets.front().offset ||
       offsets_A.front().offset > key_offsets.back().offset) &&
      (offsets_B.back().offset < key_offsets.front().offset ||
       offsets_B.front().offset > key_offsets.back().offset)) {
    return;
  }

  // Determine if this index entry constitutes a continuous feature.
  bool need_binning = false;
  for (const auto& v : key_offsets) {
    if (v.score) {
      need_binning = true;
      break;
    }
  }

  if (!need_binning) {
    // This is simple boolean feature, so we treat it's mere presence as the
    // signal.
    ProcessRange(key, offsets_A, offsets_B, key_offsets, limit_A, limit_B,
                 prior_logit, do_timestamps, output_mutex);
    return;
  }

  // If we get here, we're working with a continuous feature.  We try to find
  // an ideal partinioning point for creating strongly predictive features.
  //
  // TODO(mortehu): Implement multi-interval discretization, as described by
  // <http://ijcai.org/Past%20Proceedings/IJCAI-93-VOL2/PDF/022.pdf>.

  const auto* A_begin = &offsets_A[0];
  const auto* B_begin = &offsets_B[0];
  const auto* K_begin = &key_offsets[0];

  const auto* A_end = &offsets_A[offsets_A.size()];
  const auto* B_end = &offsets_B[offsets_B.size()];
  const auto* K_end = &key_offsets[key_offsets.size()];

  // Maps a score values to the sets they are present in.
  //   second == -1: B
  //   second ==  0: A and B
  //   second ==  1: A
  std::vector<std::pair<float, int>> classes;

  for (auto K = K_begin, A = A_begin, B = B_begin; K != K_end; ++K) {
    auto offset = K->offset;

    A = CA_offset_score_lower_bound(A, A_end, offset);
    B = CA_offset_score_lower_bound(B, B_end, offset);

    int cls = 0;
    bool match = false;

    if (A != A_end && A->offset == offset) {
      ++cls;
      match = true;
    }

    if (B != B_end && B->offset == offset) {
      --cls;
      match = true;
    }

    if (!match) continue;

    classes.emplace_back(K->score, cls);
  }

  if (classes.empty() || classes.size() < std::min(limit_A, limit_B)) return;

  std::sort(classes.begin(), classes.end());

  // Maps score values (s) to:
  //   * The number of matches in set A from -inf to s.
  //   * The number of matches in set B from -inf to s.
  //   * The number of matches in either set from -inf to s.
  std::vector<std::tuple<float, size_t, size_t, size_t>> agg;

  size_t match_count_A = 0;
  size_t match_count_B = 0;
  size_t match_count_A_or_B = 0;

  for (const auto& cls : classes) {
    if (cls.second >= 0) ++match_count_A;
    if (cls.second <= 0) ++match_count_B;
    ++match_count_A_or_B;

    if (!agg.empty() && std::get<0>(agg.back()) == cls.first) {
      std::get<1>(agg.back()) = match_count_A;
      std::get<2>(agg.back()) = match_count_B;
      std::get<3>(agg.back()) = match_count_A_or_B;
    } else {
      agg.emplace_back(cls.first, match_count_A, match_count_B,
                       match_count_A_or_B);
    }
  }

  // Iterator pointing to the best partitioning point we've found so far.
  auto best_mid = agg.begin();

  // The highest entropy gain observed so far.
  double best_mid_score = 0.0;

  for (auto mid = agg.begin(); mid != agg.end(); ++mid) {
    // Find the entropy gain of the range -inf to std::get<0>(mid).
    const auto count_A = std::get<1>(*mid);
    const auto count_B = std::get<2>(*mid);
    const auto count_A_or_B = std::get<3>(*mid);

    if (count_A >= limit_A || count_B >= limit_B) {
      const auto P_A_given_K = (count_A + 1.0) / (count_A_or_B + 2.0);
      const auto abs_logit =
          std::fabs(std::log(P_A_given_K / (1.0 - P_A_given_K)) - prior_logit);

      if (abs_logit > best_mid_score) {
        best_mid = mid;
        best_mid_score = abs_logit;
      }
    }

    // Find the entropy gain of the range std::get<0>(mid) to inf.
    const auto alt_count_A = match_count_A - std::get<1>(*mid);
    const auto alt_count_B = match_count_B - std::get<2>(*mid);
    const auto alt_count_A_or_B = match_count_A_or_B - std::get<3>(*mid);

    if (alt_count_A >= limit_A || alt_count_B >= limit_B) {
      const auto P_A_given_K = (alt_count_A + 1.0) / (alt_count_A_or_B + 2.0);
      const auto abs_logit =
          std::fabs(std::log(P_A_given_K / (1.0 - P_A_given_K)) - prior_logit);

      if (abs_logit > best_mid_score) {
        best_mid = mid;
        best_mid_score = abs_logit;
      }
    }
  }

  // The index allows multiple scores for a given item in the same index entry,
  // to allow for event based entries.  This means we can't use the values we
  // have in `agg' if we want accurate results, so we run ProcessRange() on the
  // ranges instead.

  if (best_mid == agg.begin() || (best_mid + 1) == agg.end() ||
      best_mid_score < std::log(1.05)) {
    // No subrange was more predictive than including everything.
    ProcessRange(key, offsets_A, offsets_B, key_offsets, limit_A, limit_B,
                 prior_logit, do_timestamps, output_mutex);
  } else {
    ProcessRange(key, offsets_A, offsets_B, key_offsets, limit_A, limit_B,
                 prior_logit, do_timestamps, output_mutex, -HUGE_VAL,
                 std::get<0>(*best_mid));
    ProcessRange(key, offsets_A, offsets_B, key_offsets, limit_A, limit_B,
                 prior_logit, do_timestamps, output_mutex,
                 std::get<0>(*(best_mid + 1)), HUGE_VAL);
  }
}

void FilterByTimestamp(std::vector<ca_offset_score>& keys,
                       const std::vector<ca_offset_score>& adj, float now) {
  auto i = keys.begin();
  auto j = adj.begin();

  auto output = keys.begin();

  while (i != keys.end() && j != adj.end()) {
    if (i->offset == j->offset) {
      const auto offset = i->offset;
      float min_score = j->score;

      for (;;) {
        ++j;
        if (j == adj.end() || j->offset != offset) break;
        if (j->score < min_score) min_score = j->score;
      }

      if (i->score < min_score) {
        *output = *i;
        output->score = min_score - i->score;
        ++output;
      }

      do
        ++i;
      while (i != keys.end() && i->offset == offset);

      continue;
    }

    if (i->offset < j->offset) {
      i->score = now - i->score;
      *output++ = *i++;
    } else {
      ++j;
    }
  }

  keys.erase(output, keys.end());
}

void FilterByTimestamp(std::vector<ca_offset_score>& keys,
                       const std::vector<ca_offset_score>& offsets_A,
                       const std::vector<ca_offset_score>& offsets_B) {
  auto i = keys.begin();
  auto a = offsets_A.begin();
  auto b = offsets_B.begin();

  auto output = keys.begin();

  while (i != keys.end()) {
    const auto a_ok = (a != offsets_A.end());
    const auto b_ok = (b != offsets_B.end());

    const auto offset = i->offset;
    if (a_ok && offset == a->offset) {
      auto min_score = a->score;

      for (;;) {
        ++a;
        if (a == offsets_A.end() || a->offset != offset) break;
        if (a->score < min_score) min_score = a->score;
      }

      if (i->score < min_score) {
        *output = *i;
        output->score = min_score - i->score;
        ++output;
      }
    } else if (b_ok && offset == b->offset) {
      auto min_score = b->score;

      for (;;) {
        ++b;
        if (b == offsets_B.end() || b->offset != offset) break;
        if (b->score < min_score) min_score = b->score;
      }

      if (i->score < min_score) {
        *output = *i;
        output->score = min_score - i->score;
        ++output;
      }
    } else if (a_ok && a->offset < offset) {
      do
        ++a;
      while (a != offsets_A.end() && a->offset < offset);
      continue;
    } else if (b_ok && b->offset < offset) {
      do
        ++b;
      while (b != offsets_B.end() && b->offset < offset);
      continue;
    }

    do
      ++i;
    while (i != keys.end() && i->offset == offset);
  }

  keys.erase(output, keys.end());
}

std::string PrimaryKeywordForQuery(const Query* query) {
  if (query->type == kQueryLeaf) return query->identifier;

  if (query->lhs) return PrimaryKeywordForQuery(query->lhs);
  if (query->rhs) return PrimaryKeywordForQuery(query->rhs);

  return std::string();
}

}  // namespace

// Prints the features that are more predictive of items in set A than set B.
//
// Parameters:
//
//   schema: Schema object used to retrieve list of index tables.
//   query_A: Query returning set A.
//   query_B: Query returning set B.
void ca_schema_query_correlate(Schema* schema, const Query* query_A,
                               const Query* query_B) {
  const auto& keywords = Keywords::GetInstance();

  // If query A's primary keyword is timestamped, we'll discard information
  // that was unavailable at the time of the event.
  const auto a_is_timestamped =
      keywords.IsTimestamped(PrimaryKeywordForQuery(query_A));
  const auto b_is_timestamped =
      keywords.IsTimestamped(PrimaryKeywordForQuery(query_B));

  std::vector<ca_offset_score> offsets_A, offsets_B;

  ProcessQuery(offsets_A, query_A, schema, false, false);
  ProcessQuery(offsets_B, query_B, schema, false, false);

  offsets_B.resize(SubtractOffsets(&offsets_B[0], offsets_B.size(),
                                             &offsets_A[0], offsets_A.size()));

  if (offsets_A.empty() || offsets_B.empty()) return;

  // The prior probability of an item from either set belonging to set A.  If
  // set A is larger than set B, a random coin flip landing on heads is
  // predictive of set A, so we need to correct for the prior probability to
  // determine what features are actually interesting.
  //
  // We use log-odds because of their additive properties.  For example, "5
  // times more likely" is the same as adding log(5.0) to your log-odds.
  // Subtracting posterior log-odds for a feature from the prior gives you the
  // information gain of that feature.
  const auto prior_logit =
      std::log((offsets_A.size() + 1.0) / (offsets_B.size() + 1.0));

  // If a feature isn't present in at least 5% of either set, we're not
  // interested in it.
  const auto limit_A = std::max(offsets_A.size() / 20, 1UL);
  const auto limit_B = std::max(offsets_B.size() / 20, 1UL);

  const auto now = time(nullptr) / 86400.0f;

  evenk::thread_pool<thread_pool_queue> thread_pool(std::thread::hardware_concurrency());

  std::vector<ca_offset_score> key_offsets;

  // Mutex controlling access to stdout.
  std::mutex output_mutex;

  for (auto& index_table : schema->IndexTables()) {
    index_table.table->SeekToFirst();

    string_view key, data;

    while (index_table.table->ReadRow(key, data)) {
      if (a_is_timestamped && keywords.IsEphemeral(key)) continue;

      key_offsets.clear();

      ca_offset_score_parse(data, &key_offsets);

      if (key_offsets.size() < limit_A && key_offsets.size() < limit_B)
        continue;

      thread_pool.submit([
        key = key.to_string(),
        key_offsets = std::move(key_offsets),
        &offsets_A,
        &offsets_B,
        limit_A,
        limit_B,
        prior_logit,
        &keywords,
        a_is_timestamped,
        b_is_timestamped,
        now,
        &output_mutex
      ]() mutable {
        if (a_is_timestamped && keywords.IsTimestamped(key)) {
          if (b_is_timestamped)
            FilterByTimestamp(key_offsets, offsets_A, offsets_B);
          else
            FilterByTimestamp(key_offsets, offsets_A, now);
        }

        ProcessSeries(std::move(key), std::move(key_offsets), offsets_A,
                      offsets_B, limit_A, limit_B, prior_logit,
                      a_is_timestamped, output_mutex);
      });
    }
  }

  thread_pool.wait();
}

}  // namespace table
}  // namespace cantera
