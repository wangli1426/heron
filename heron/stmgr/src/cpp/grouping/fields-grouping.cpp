/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "grouping/fields-grouping.h"
#include <iostream>
#include <list>
#include <vector>
#include "grouping/grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

FieldsGrouping::FieldsGrouping(const proto::api::InputStream& _is,
                               const proto::api::StreamSchema& _schema,
                               const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {
  for (sp_int32 i = 0; i < _schema.keys_size(); ++i) {
    for (sp_int32 j = 0; j < _is.grouping_fields().keys_size(); ++j) {
      if (_schema.keys(i).key() == _is.grouping_fields().keys(j).key()) {
        fields_grouping_indices_.push_back(i);
        break;
      }
    }
  }
}

FieldsGrouping::~FieldsGrouping() {}

void FieldsGrouping::GetListToSend(const proto::system::HeronDataTuple& _tuple,
                                   std::vector<sp_int32>& _return) {
  sp_int32 task_index = 0;
  size_t prime_num = 633910111UL;
  for (std::list<sp_int32>::iterator iter = fields_grouping_indices_.begin();
       iter != fields_grouping_indices_.end(); ++iter) {
    CHECK(_tuple.values_size() > *iter);
    size_t h = str_hash_fn(_tuple.values(*iter));
    task_index += (h % prime_num);
  }
  task_index = task_index % task_ids_.size();
  _return.push_back(task_ids_[task_index]);
}

void FieldsGrouping::HandCodedGetListToSend(const proto::system::HeronDataTuple& _tuple,
                                   std::vector<sp_int32>& _return) {
  sp_int32 task_index = 0;
  size_t prime_num = 633910111UL;
//  for (std::list<sp_int32>::iterator iter = fields_grouping_indices_.begin();
//       iter != fields_grouping_indices_.end(); ++iter) {
  for (sp_int32 i = 0; i < 5; i++) {
    CHECK_GT(_tuple.values_size(), 0);
    size_t h = str_hash_fn(_tuple.values(i));
//    size_t h = *(reinterpret_cast<const size_t*>(_tuple.values(0).c_str()));
    task_index = (h % prime_num);
  }
//
//    CHECK_GT(_tuple.values_size(), 0);
//    h = str_hash_fn(_tuple.values(1));
//    task_index += (h % prime_num);
//
//    CHECK_GT(_tuple.values_size(), 0);
//    h = str_hash_fn(_tuple.values(2));
//    task_index += (h % prime_num);
//
//    CHECK_GT(_tuple.values_size(), 0);
//    h = str_hash_fn(_tuple.values(3));
//    task_index += (h % prime_num);
//
//    CHECK_GT(_tuple.values_size(), 0);
//    h = str_hash_fn(_tuple.values(4));
//    task_index += (h % prime_num);

//  }
  task_index = task_index % task_ids_.size();
  _return.push_back(task_ids_[task_index]);
}

}  // namespace stmgr
}  // namespace heron
