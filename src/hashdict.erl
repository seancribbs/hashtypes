-module(hashdict).

-define(ordered_threshold, 8).
-record(ordered, { size = 0, bucket= [] }).

-define(expand_load, 5).
-define(contract_load, 2).
-define(node_bitmap, 2#111).
-define(node_shift, 3).
-define(node_size, 8).
-define(node_template, erlang:make_tuple(?node_size, [])).

-record(trie, {size = 0,
               depth = 0,
               expand_on = ?node_size * ?expand_load,
               contract_on = ?contract_load,
               root = ?node_template }).

-compile(inline_list_funcs).
-compile({inline, [bucket_hash/1,
                   bucket_index/1,
                   bucket_nth_index/2,
                   bucket_next/1]}).

-define(is_hashdict(A), is_record(A, trie) orelse is_record(A, ordered)).

-export([append/3,
         append_list/3,
         erase/2,
         fetch/2,
         fetch_keys/1,
         filter/2,
         find/2,
         fold/3,
         from_list/1,
         is_key/2,
         map/2,
         merge/3,
         new/0,
         size/1,
         store/3,
         to_list/1,
         update/3,
         update/4,
         update_counter/3
        ]).

-export_type([hashdict/0]).
-opaque hashdict() :: #ordered{} | #trie{}.

-spec append(term(), term(), hashdict()) -> hashdict().
append(Key, Value, HashDict) when ?is_hashdict(HashDict) ->
    append_list(Key, [Value], HashDict).

-spec append_list(term(), [term()], hashdict()) -> hashdict().
append_list(Key, ValList, HashDict) when is_list(ValList), ?is_hashdict(HashDict) ->
    update(Key,
           fun(L) when is_list(L) ->
                   L ++ ValList;
              (_) -> erlang:error(badarg)
           end, [], HashDict).

-spec fetch(term(), hashdict()) -> term().
fetch(Key, HashDict) when ?is_hashdict(HashDict) ->
    case find(Key, HashDict) of
        {ok, Value} -> Value;
        error -> erlang:error(badarg)
    end.

-spec fetch_keys(hashdict()) -> [term()].
fetch_keys(HashDict) when ?is_hashdict(HashDict) ->
    fold(fun(K, _V, Acc) -> [K|Acc] end, [], HashDict).

-spec filter(fun((Key::term(), Value::term()) -> boolean()), hashdict()) -> hashdict().
filter(Pred, HashDict) when ?is_hashdict(HashDict) ->
    fold(fun(K, V, Acc) ->
                 case Pred(K,V) of
                     true -> Acc;
                     false -> erase(K, Acc)
                 end
         end, HashDict, HashDict).

-spec find(term(), hashdict()) -> {ok, term()} | error.
find(Key, #ordered{bucket=Bucket}) ->
    bucket_get(Key, Bucket);
find(Key, #trie{root=Root, depth=Depth}) ->
    bucket_get(Key, node_bucket(Root, Depth, bucket_hash(Key))).

-spec fold(fun((Key::term(), Value::term(), Acc0::term()) -> Acc1::term()),
           Acc::term(), hashdict()) -> term().
fold(Fun, Acc0, #ordered{bucket=Bucket}) ->
    bucket_fold(Bucket, Acc0, Fun);
fold(Fun, Acc0, #trie{root=Root, depth=Depth}) ->
    node_fold(Root, Depth, Acc0, Fun, ?node_size).

-spec from_list([{term(), term()}]) -> hashdict().
from_list(Pairs) ->
    lists:foldl(fun({K,V},HashDict) ->
                        store(K, V, HashDict)
                end,
                #ordered{}, Pairs).

-spec is_key(term(), hashdict()) -> boolean().
is_key(Key, HashDict) when ?is_hashdict(HashDict) ->
    find(Key, HashDict) /= error.

-spec map(fun((Key :: term(), Value1 :: term()) -> Value2 :: term()), hashdict()) -> hashdict().
map(Fun, HashDict) when ?is_hashdict(HashDict) ->
    fold(fun(K,V,Acc) ->
                 store(K, Fun(K,V), Acc)
         end, new(), HashDict).

-spec merge(fun((Key :: term(), Value1 :: term(), Value2 :: term()) -> Value :: term()),
            hashdict(), hashdict()) -> Dict3::hashdict().
merge(Fun, Dict1, Dict2) when ?is_hashdict(Dict1), ?is_hashdict(Dict2) ->
  fold(fun (K, V1, D) ->
                 update(K, fun(V2) -> Fun(K, V1, V2) end, V1, D)
         end, Dict2, Dict1).                                    

-spec new() -> hashdict().
new() ->
    #ordered{}.

-spec size(hashdict()) -> non_neg_integer().
size(#ordered{size=S}) -> S;
size(#trie{size=S}) -> S.

-spec store(term(), term(), hashdict()) -> hashdict().
store(Key, Value, Dict) when ?is_hashdict(Dict) ->
    dict_put(Key, {put, Value}, Dict).

-spec to_list(hashdict()) -> [{term(), term()}].
to_list(HashDict) when ?is_hashdict(HashDict) ->
    fold(fun(K,V,Acc) ->
                 [{K,V}|Acc]
         end, [], HashDict).
            
erase(Key, #ordered{bucket=Bucket, size=Size}=HashDict) ->
    case bucket_delete(Bucket, Key) of
        {_, _Value, 0} ->
            HashDict;
        {NewBucket, _Value, -1} ->
            HashDict#ordered{size=Size-1, bucket=NewBucket}
    end;
erase(Key, #trie{root=Root, size=Size, depth=Depth}=HashDict) ->
    Pos = bucket_hash(Key),
    case node_delete(Root, Depth, Pos, Key) of
        {_, _Value, 0} -> HashDict;
        {Root1, _Value, -1} ->
            if Depth > 0 andalso
               HashDict#trie.contract_on == Size ->
                    Root2 = node_contract(Root1, Depth),
                    HashDict#trie{root=Root2, size=Size-1, depth=Depth-1,
                                  contract_on=Size div ?node_size,
                                  expand_on=HashDict#trie.expand_on div ?node_size};
               true ->
                    HashDict#trie{size=Size-1, root=Root1}
            end
    end.

-spec update(term(), fun((term()) -> term()), hashdict()) -> hashdict().
update(Key, Fun, HashDict) when ?is_hashdict(HashDict) ->    
    store(Key, Fun(fetch(Key, HashDict)), HashDict).

-spec update(term(), fun((term()) -> term()), term(), hashdict()) -> hashdict().
update(Key, Fun, Initial, HashDict) when ?is_hashdict(HashDict) ->
    dict_put(Key, {update, Initial, Fun}, HashDict).

-spec update_counter(term(), number(), hashdict()) -> hashdict().
update_counter(Key, Incr, D) when ?is_hashdict(D), 
                                  is_number(Incr) ->
    update(Key, fun (Old) -> Old + Incr end, Incr, D).


%% Internal functions
dict_put(Key, Value, #ordered{size=?ordered_threshold, bucket=Bucket}) ->
    Root = node_relocate(Bucket, 0),
    dict_put(Key, Value, #trie{size=?ordered_threshold, root=Root});
dict_put(Key, Value, #ordered{size=Size, bucket=Bucket}=Dict) ->
    {New, Count} = bucket_put(Bucket, Key, Value),
    Dict#ordered{size=Size+Count, bucket=New};
dict_put(Key, Value, #trie{root=Root0, depth=Depth, size=Size,
                           expand_on=Size, contract_on=ContractOn}=Dict0) ->
    Root = node_expand(Root0, Depth, Depth + 1),
    Dict = Dict0#trie{root=Root, depth=Depth+1, expand_on=Size*?node_size,
                      contract_on=ContractOn*?node_size},
    dict_put(Key, Value, Dict);
dict_put(Key, Value, #trie{root=Root0, size=Size, depth=Depth}=Dict0) ->
    Pos = bucket_hash(Key),
    {Root, Count} = node_put(Root0, Depth, Pos, Key, Value),
    Dict0#trie{size=Size+Count, root=Root}.

%% Bucket helpers
bucket_get([[K|_]|_Bucket], Key) when K > Key ->
    error;
bucket_get([[Key|Value]|_Bucket], Key) ->
    {ok, Value};
bucket_get([_E|Bucket], Key) ->
    bucket_get(Bucket, Key);
bucket_get([], _Key) ->
    error.

bucket_put([[K|_]|Bucket], Key, {put, Value}) when K > Key ->
    {[[Key|Value]|Bucket], 1};
bucket_put([[K|_]|Bucket], Key, {update, Initial, _Fun}) when K > Key ->
    {[[Key|Initial]|Bucket], 1};
bucket_put([[Key|_]|Bucket], Key, {put, Value}) ->
    {[[Key|Value]|Bucket], 0};
bucket_put([[Key|Value]|Bucket], Key, {update, _Initial, Fun}) ->
    {[[Key|Fun(Value)]|Bucket], 0};
bucket_put([E|Bucket], Key, Value) ->
    {Rest, Count} = bucket_put(Bucket, Key, Value),
    {[E|Rest], Count};
bucket_put([], Key, {put, Value}) ->
    {[[Key|Value]], 1};
bucket_put([], Key, {update, Initial, _Fun}) ->
    {[[Key|Initial]], 1}.


bucket_put_b([[K|_]|_]=Bucket, Key, Value) when K > Key ->
    [[Key|Value]|Bucket];
bucket_put_b([[Key|_]|Bucket], Key, Value) ->
    [[Key|Value]|Bucket];
bucket_put_b([E|Bucket], Key, Value) ->
    [E|bucket_put_b(Bucket, Key, Value)];
bucket_put_b([], Key, Value) -> [[Key|Value]].

bucket_delete([[K|_]|_]=Bucket, Key) when K > Key ->
    {Bucket, undefined, 0};
bucket_delete([[Key|Value]|Bucket], Key) ->
    {Bucket, Value, -1};
bucket_delete([E|Bucket], Key) ->
    {Rest, Value, Count} = bucket_delete(Bucket, Key),
    {[E|Rest], Value, Count};
bucket_delete([], _Key) ->
    {[], undefined, 0}.

%% bucket_reduce([[K|V]|T], Acc, Fun) ->
%%     bucket_reduce(T, Fun({K,V}, Acc), Fun);
%% bucket_reduce([], Acc, _Fun) -> Acc.

bucket_fold(Bucket, Acc, Fun) ->
    lists:foldl(Fun, Acc, Bucket).

bucket_hash(Key) ->
    erlang:phash2(Key).

bucket_index(Hash) ->
    Hash band ?node_bitmap.

bucket_nth_index(Hash, N) ->
    (Hash bsr (?node_shift * N)) band ?node_bitmap.

bucket_next(Hash) ->
    Hash bsr ?node_shift.

%% Node/trie helpers
node_bucket(Node, 0, Hash) ->
    element(bucket_index(Hash), Node);
node_bucket(Node, Depth, Hash) ->
    Child = element(bucket_index(Hash), Node),
    node_bucket(Child, Depth-1, bucket_next(Hash)).

node_put(Node, 0, Hash, Key, Value) ->
    Pos = bucket_index(Hash),
    {New, Count} = bucket_put(element(Pos, Node), Key, Value),
    {setelement(Pos, Node, New), Count};
node_put(Node, Depth, Hash, Key, Value) ->
    Pos = bucket_index(Hash),
    {New, Count} = node_put(element(Pos, Node), Depth - 1, bucket_next(Hash), Key, Value),
    {setelement(Pos, Node, New), Count}.

node_delete(Node, 0, Hash, Key) ->
    Pos = bucket_index(Hash),
    case bucket_delete(element(Pos, Node), Key) of
        {_, Value, 0} -> {Node, Value, 0};
        {New, Value, -1} -> {setelement(Pos, Node, New), Value, -1}
    end;
node_delete(Node, Depth, Hash, Key) ->
    Pos = bucket_index(Hash),
    case node_delete(element(Pos, Node), Depth - 1, bucket_next(Hash), Key) of
        {_, Value, 0} -> {Node, Value, 0};
        {New, Value, -1} -> {setelement(Pos, Node, New), Value, -1}
    end.

%% node_reduce(Bucket, -1, Acc, Fun, _) ->
%%     bucket_reduce(Bucket, Acc, Fun);
%% node_reduce(Node, Depth, Acc0, Fun, Count) when Count >= 1 ->
%%     Acc = node_reduce(element(Count, Node), Depth - 1, Acc0, Fun, ?node_size),
%%     node_reduce(Node, Depth, Acc, Fun, Count - 1);
%% node_reduce(_Node, _Depth, Acc, _Fun, 0) ->
%%     Acc.

node_fold(Bucket, -1, Acc, Fun, _) ->
    bucket_fold(Bucket, Acc, Fun);
node_fold(Node, Depth, Acc0, Fun, Count) when Count >= 1 ->
    Acc = node_fold(element(Count, Node), Depth - 1, Acc0, Fun, ?node_size),
    node_fold(Node, Depth, Acc, Fun, Count - 1);
node_fold(_Node, _, Acc, _Fun, 0) ->
    Acc.

%% Node resizing
node_expand({ B1, B2, B3, B4, B5, B6, B7, B8 }, 0, N) ->
    { node_relocate(B1, N), node_relocate(B2, N), node_relocate(B3, N),
      node_relocate(B4, N), node_relocate(B5, N), node_relocate(B6, N),
      node_relocate(B7, N), node_relocate(B8, N) };
node_expand({ B1, B2, B3, B4, B5, B6, B7, B8 }, Depth0, N) ->
    Depth = Depth0 - 1,
    { node_expand(B1, Depth, N), node_expand(B2, Depth, N), node_expand(B3, Depth, N),
      node_expand(B4, Depth, N), node_expand(B5, Depth, N), node_expand(B6, Depth, N),
      node_expand(B7, Depth, N), node_expand(B8, Depth, N) }.


node_contract({ B1, B2, B3, B4, B5, B6, B7, B8 }, Depth0) when Depth0 > 0 ->
    Depth = Depth0 - 1,
    { node_contract(B1, Depth), node_contract(B2, Depth), node_contract(B3, Depth),
      node_contract(B4, Depth), node_contract(B5, Depth), node_contract(B6, Depth),
      node_contract(B7, Depth), node_contract(B8, Depth) };

node_contract({ B1, _B2, _B3, _B4, _B5, _B6, _B7, _B8 }=Tuple, 0) ->
    lists:foldl(fun each_contract/2, B1, tl(tuple_to_list(Tuple))).

%% TODO reorder arguments, Acc should be second, or do the ugly nesting in node_contract
each_contract([[K|_V]=E|Acc], [[Key|_Value]|_]=Bucket) when K < Key ->
    [E|each_contract(Acc, Bucket)];
each_contract(Acc, [E|Bucket]) ->
    [E|each_contract(Acc, Bucket)];
each_contract([], Bucket) -> Bucket;
each_contract(Acc, []) -> Acc.


node_relocate(Bucket, N) ->
    node_relocate(?node_template, Bucket, N).

node_relocate(Node, Bucket, N) ->
    lists:foldl(fun([Key|Value], Acc) ->
                        Pos = bucket_nth_index(bucket_hash(Key), N),
                        setelement(Pos, Acc, bucket_put_b(element(Pos, Acc), Key, Value))
                end, Node, Bucket).
