# hashtypes

A port of Elixir's HashDict and HashSet to plain Erlang. These types
provide greater scalability across different element counts than
Erlang's standard library, automatically switching between
ordered/list types and nested/hash types at well-tuned thresholds.

**NOTE**: This is incomplete and untested, as it is a straight port. I
will be adding QuickCheck properties that test the gamut of API operations.

## TODO

* Finish porting HashSet
* eunit smoke tests
* QuickCheck properties for set and dict, test against stdlib as well

## Copyright and License

Copyright 2013-2014, Sean Cribbs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
