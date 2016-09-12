% ---------------------------------------------------------------------------
%   Copyright (C) 2008 0x6e6562
%
%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.
% ---------------------------------------------------------------------------

-module(hessian).
-behaviour(gen_server).
-author("wanggaoquan@gmail.com").
-modifiedBy("langyong135@gmail.com").

-export([start/0, start_link/0, stop/0, dispatch/2, dispatch/3, call/3, register_module/2, register_modules/1, register_record/3, register_records/1]).
-export([behaviour_info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% input arguments map
%% Hessian    ----->       Erlang
%% null       ----->       null
%% true       ----->       true
%% false      ----->       false
%% int        ----->       integer
%% long       ----->       integer
%% double     ----->       float
%% date       ----->       {MegaSecs, Secs, MicroSecs}
%% string     ----->       character list(utf8 list)
%% xml        ----->       binary
%% binary     ----->       binary
%% list       ----->       list
%% map        ----->       record or [{key1, value1},{key2, value2}...]
%% ref        ----->       not implement
%% remote     ----->       not implement
%%
%% Return value map
%% Hessian    <-----       Erlang
%% null       <-----       null
%% true       <-----       true
%% false      <-----       false
%% int        <-----       integer
%% long       <-----       integer
%% double     <-----       float
%% date       <-----       {{MegaSecs, Secs, MicroSecs}, date}
%% string     <-----       list
%% binary     <-----       binary
%% xml        <-----       {BinaryValue, xml}
%% list       <-----       {ListValue, list} or list
%% map        <-----       record or {[{key1, value1},{key2, value2}...], map}
%% ref        <-----       not implement
%% remote     <-----       not implement
%%

-define(DEBUG, 1).

-ifdef(DEBUG).
-define(TRACE(F,A), io:format(F, A)).
-else.
-define(TRACE(F,A), true).
-endif.

%% help function
writelog(Log, Format, Args) ->
	if is_function(Log) ->
		Log(Format, Args);
	true ->
		?TRACE(Format, Args)
	end.

send_call(Data, Url) ->
	{ok, {{_Version, _ReplyCode, _ReasonPhrase}, _Headers, Response}} =
		httpc:request(post, {Url, [], "x-application/hessian", Data}, [], []),
	list_to_binary(Response).

%% behavour defination
behaviour_info(callbacks) ->
	[{hessian_init, 1}, {hessian_close, 1}, {hessian_call, 3}].

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
	gen_server:start({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:cast(?MODULE, stop).

%% gen_server callback functions
init(_) ->
  io:format(">>>>>>>>>>>>>> nit:"),
	case inets:start() of
		ok ->
			ok;
		{error,{already_started,inets}} ->
			ok
	end,

	%% Table map Url --> Module
	ets:new(hessian_url2mod_map, [public,named_table]),

	%%Table map External Object to internal record 
	ets:new(hessian_record_ext2int_map, [public,named_table]),

	%%Table map internal record to external object
	ets:new(hessian_record_int2ext_map, [public,named_table]),
	{ok, 0}.

handle_call({register_module, Module, Arg}, _From, State) ->
	Res = case (catch Module:hessian_init(Arg)) of
		{ok, Url, ModuleState} ->
			ets:insert(hessian_url2mod_map, {Url, Module, ModuleState});
		_Other ->
			?TRACE("register_module: ~p~n", [_Other]),
			false
	end,
	{reply, Res, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.
	
handle_cast(stop, State) ->
	ets:foldl(fun({_Url, Module, ModuleState}, _Acc)->
		catch Module:hessian_close(ModuleState)
		end, 0, hessian_url2mod_map),
	{stop, normal, State};
handle_cast(_Request, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% hessian module and function and record managment functions
register_module(Module, Arg) ->
	gen_server:call(?MODULE, {register_module, Module, Arg}).

register_modules([{M,A}|L]) ->
	register_module(M, A),
	register_modules(L);
register_modules([]) ->
	ok.

register_record(ExternName, Fields, DefValue) ->
	[RecordName|Def] = tuple_to_list(DefValue),
	ets:insert(hessian_record_int2ext_map, {RecordName, {ExternName,Fields}}),
	KeyValue = lists:zip(Fields, Def),
	ets:insert(hessian_record_ext2int_map, {ExternName, {RecordName,KeyValue}}).

register_records([{ExternName, Fields, DefValue}|L]) ->
	register_record(ExternName, Fields, DefValue),
	register_records(L);
register_records([]) ->
	ok.

%% decode functions

decode_str(Rest, 0, R) ->
	{lists:reverse(R), Rest}; 
decode_str(<<C/utf8, Rest/binary>>, N, R) ->
	decode_str(Rest, N-1, [C|R]).

decode(<<$S, Len:16/unsigned, Rest/binary>>) ->
	decode_str(Rest, Len, []);

decode(<<$B, Len:16/unsigned, Rest/binary>>) ->
	<<Bin:Len/binary, Rest2/binary>> = Rest,
	{Bin, Rest2};
decode(<<$b, Len:16/unsigned, Rest/binary>>) ->
	<<Bin:Len/binary, Rest2/binary>> = Rest,
	decode_bin(Rest2, [Bin]);

decode(<<$I, Int:32/signed, Rest/binary>>) -> {Int, Rest};
decode(<<$L, Long:64/signed, Rest/binary>>) -> {Long, Rest};
decode(<<$D, Float/float, Rest/binary>>) -> {Float, Rest};
decode(<<$T,Rest/binary>>) -> {true, Rest};
decode(<<$F,Rest/binary>>) -> {false, Rest};
decode(<<$N,Rest/binary>>) -> {null, Rest};

decode(<<$V, $t, Len:16/unsigned, Rest/binary>>) ->
<<_Type:Len/binary, Rest2/binary>> = Rest,
	decode_list(Rest2, []);
decode(<<$V, Rest/binary>>) ->
decode_list(Rest, []);

decode(<<$M, $t, Len:16/unsigned, Rest/binary>>) ->
	<<ObjName:Len/binary, Rest2/binary>> = Rest,
	{Map, Rest3} = decode_map(Rest2, []),
	case ets:lookup(hessian_record_ext2int_map, binary_to_list(ObjName)) of
		[{_, {RecordName,KeyValues}}] when is_list(KeyValues) ->
			Res = lists:foldl(fun({XK, V}, Rs) ->
				K = list_to_existing_atom(XK),
				lists:keyreplace(K, 1, Rs, {K,V})
				end, KeyValues, Map),

			{_, Vals} = lists:unzip(Res),
			{list_to_tuple([RecordName|Vals]), Rest3};
		[] ->
			{Map, Rest3}
	end;
decode(<<$M, Rest/binary>>) ->
	decode_map(Rest, []);

decode(<<$c, 1, 0, $m, Len:16/unsigned, Rest/binary>>) ->
	<<Func:Len/binary, Rest2/binary>> = Rest,
	{Args, Rest3} = decode_list(Rest2, []),
	{{binary_to_list(Func), Args}, Rest3};

decode(<<$d, Date:64/unsigned, Rest/binary>>) ->
	MegaSecs = Date div 1000000000,
	Secs = (Date rem 1000000000) div 1000,
	MicroSecs = (Date rem 1000000) * 1000,
	{{MegaSecs, Secs, MicroSecs}, Rest};

decode(<<$X, Len:16/unsigned, Rest/binary>>) ->
	<<Str:Len/binary, Rest2/binary>> = Rest,
	{unicode:characters_to_list(Str), Rest2};

decode(<<$f, Rest/binary>>) ->
	{Fault, Rest2} = decode_map(Rest, []),
	{{Fault, fault}, Rest2};

decode(<<$r, _Rest/binary>>) ->
	throw({'RequireHeaderException', "Remote type not supported!"});
decode(<<$R, _Rest/binary>>) ->
	throw({'RequireHeaderException', "Ref type not supported!"}).

decode_list(<<$z, Rest/binary>>, R) ->
	{lists:reverse(R), Rest};
decode_list(<<$l, _Len:32/unsigned, Rest/binary>>, R) ->
	decode_list(Rest, R);
decode_list(Bin, R) ->
	{E, Rest} = decode(Bin),
	decode_list(Rest, [E|R]).

decode_map(<<$z, Rest/binary>>, R) ->
	{lists:reverse(R), Rest};
decode_map(Bin, R) ->
  {Key, Rest} = decode(Bin),
  {Value, Rest2} = decode(Rest),
  %decode_map(Rest2, [{Key,Value}|R]).
  CorrectKey = try
                 unicode:characters_to_binary(Key)
               catch
                 error:badarg ->
                   Key
               end,
  if is_list(Value) and is_integer(hd(Value)) ->
     try
       decode_map(Rest2, [{CorrectKey,unicode:characters_to_binary(Value)}|R])
     catch
       error:badarg ->
         Value
     end;
    is_list(Value) and is_list(hd(Value)) and is_integer(hd(hd(Value))) ->
       Fun = lists:map(fun(A) -> unicode:characters_to_binary(A) end, Value), 
       decode_map(Rest2, [{CorrectKey, Fun}|R]);
    true ->
       decode_map(Rest2, [{CorrectKey,Value}|R])
   end.

decode_bin(<<$b, Len:16/unsigned, Rest/binary>>, R) ->
	<<Bin:Len, Rest2/binary>> = Rest,
	decode_bin(Rest2, [Bin|R]);
decode_bin(<<$B, Len:16/unsigned, Rest/binary>>, R) ->
	<<Bin:Len, Rest2/binary>> = Rest,
	{iolist_to_binary(lists:reverse([Bin|R])), Rest2}.


%% encode functions
to_string(AtomList)  when is_list(AtomList) ->
    to_string(AtomList,"");
to_string( _)  ->
    {error,error_type}.

to_string([], R) -> lists:reverse(R);
to_string([H|T], R)  when is_atom(H) ->
    to_string(T,atom_to_list(H) ++ R);
to_string([H|T], R)  when is_list(H) ->
    to_string(T,H ++ R);
to_string(_, _)  ->
    {error,error_type}.
is_string([H|T]) ->
	if is_integer(H) ->
		is_string(T);
	true ->
		false
	end;
is_string([]) ->
	true.

encode(Int, R) when is_integer(Int) ->
	if Int >= -2147483648 andalso Int =< 2147483647 ->
		[<<$I, Int:32/signed>>|R];
	true ->
		[<<$L, Int:64/signed>>|R]
	end;
encode(F, R) when is_float(F) ->
	[<<$D, F/float>>|R];

encode(L, R) when is_list(L) ->
	case is_string(L) of
		true ->
			Len = length(L),
			Str = unicode:characters_to_binary(L),
			[<<$S, Len:16/unsigned, Str/binary>>|R];
		false ->
			encode_list(L, [$V|R])
	end;
encode({L, string}, R) when is_list(L) ->
	Len = length(L),
	Str = unicode:characters_to_binary(L),
	[<<$S, Len:16/unsigned, Str/binary>>|R];

encode(B, R) when is_binary(B) ->
	encode_bin(B, R);

encode(true, R) ->
	[$T|R];
encode(false, R) ->
	[$F|R];
encode(null, R) ->
	[$N|R];

encode({Localtime, localtime}, R) ->
	case calendar:local_time_to_universal_time_dst(Localtime) of
	[_, Dt] ->
		encode({Dt, datetime}, R);
	[Dt] ->
		encode({Dt, datetime}, R)
	end;
encode({Datetime, datetime}, R) ->
	MillSecs = (calendar:datetime_to_gregorian_seconds(Datetime) - 62167219200) * 1000,
	[<<$d, MillSecs:64/unsigned>>|R];
encode({{MegaSecs, Secs, MicroSecs}, now}, R) ->
	MillSecs = MegaSecs*1000000000 + Secs * 1000 + (MicroSecs div 1000),
	[<<$d, MillSecs:64/unsigned>>|R];
encode({Secs, epoch}, R) when is_integer(Secs) ->
	MillSecs = Secs * 1000,
	[<<$d, MillSecs:64/unsigned>>|R];

encode({L, list}, R) when is_list(L) ->
	encode_list(L, [$V|R]);

encode({L, XName, map}, R) when is_list(L) ->
	Name = if is_atom(XName) ->
			list_to_binary(atom_to_list(XName));
		is_list(XName) ->
			list_to_binary(XName);
		is_binary(XName) ->
			XName
		end,
	Len = size(Name),
	encode_map(L, [<<$M,$t,Len:16/unsigned,Name/binary>>|R]);

encode({L, map}, R) when is_list(L) ->
	encode_map(L, [$M|R]);

encode(M, R) when is_tuple(M) ->
	[RecordName|Vals] = tuple_to_list(M),
	case ets:lookup(hessian_record_int2ext_map, RecordName) of
		[{RecordName, {ExternName, Fields}}] ->
			Map = lists:zip(Fields, Vals),
			encode({Map, ExternName, map}, R)
	end.

encode_list([H|L], R) ->
	E = encode(H, R),
	encode_list(L, E);
encode_list([], R) ->
	[$z|R].

encode_map([{XKey, Val}|L], R) ->
	Key = if is_atom(XKey) ->
			atom_to_list(XKey);
		true ->
			XKey
		end,
	R1 = encode(Key, R),
	R2 = encode(Val, R1),
	encode_map(L, R2);
encode_map([], R) ->
	[$z|R].

encode_bin(B, R) when size(B) < 32768 ->
	Size = size(B),
	[<<$B, Size:16/unsigned, B/binary>>|R];
encode_bin(<<B:262144/binary,Rest/binary>>, R) ->
	Size = size(B),
	encode_bin(Rest, [<<$b, Size:16/unsigned, B/binary>>|R]).

encode_reply(Val) ->
	E = encode(Val, [0,1,$r]),
	iolist_to_binary(lists:reverse([$z|E])).

encode_fault(Code, Msg, XDetail) ->
	Detail = if length(XDetail) > 256 ->
			lists:sublist(XDetail, 256);
		true ->
			XDetail
		end,

	M = [{"code", Code}, {"message", Msg}, {"detail", Detail}],
	R1 = [<<$r, 1, 0, $f>>],
	R2 = encode_map(M, R1),
	iolist_to_binary(lists:reverse([$z|R2])).


%% hessian client function
call(Method, Args, SendFun) when is_function(SendFun) ->
	Len = length(Method),
	BMethod = list_to_binary(Method),
	R1 = [<<$c, 1, 0, $m, Len:16/unsigned,  BMethod/binary>>],
	R2 = encode_list(Args, R1),
	<<$r,1,0,R3/binary>> = SendFun(iolist_to_binary(lists:reverse(R2))),
	{R4, <<$z>>} = decode(R3),
	R4;
call(Method, Args, Url) when is_list(Url) ->
	call(Method, Args, fun(Data) -> send_call(Data, Url) end).

%% hessian server function
dispatch(Url, Bin, Log) ->
	case ets:lookup(hessian_url2mod_map, Url) of
		[{Url, Module, State}] ->
			case (catch decode(Bin)) of
				{{ExtFunc, Args}, <<>>} ->
					case (catch Module:hessian_call(list_to_existing_atom(ExtFunc), Args, State)) of
						{reply, Result} ->
							case (catch encode_reply(Result)) of
								{'EXIT', Err} ->
									writelog(Log, "~p:~s(~p) return invalid data: ~p~n", [Module,ExtFunc,Args,Result]),
									ErrStr = lists:flatten(io_lib:format("~p",[Err])),
									encode_fault("ServiceException", "The called method threw an exception.", ErrStr);
								Res ->
									Res
							end;
						{error, Code, Message} ->
							encode_fault(Code, Message, "");
						{'EXIT', {undef, Err}} ->
							writelog(Log, "~p:~s(~p) not matched~n", [Module,ExtFunc,Args]),
							ErrStr = lists:flatten(io_lib:format("~p",[Err])),
							encode_fault("NoSuchMethodException", "The requested method does not exist.", ErrStr);
						{'EXIT', Err}->
							ErrStr = lists:flatten(io_lib:format("~p",[Err])),
							writelog(Log, "~p:~s(~p) Exception: ~s~n", [Module,ExtFunc,Args, ErrStr]),
							encode_fault("ServiceException", "The called method threw an exception.", ErrStr); 
						Res ->
							writelog(Log, "~p:~s(~p) return misunderstanded: ~p~n", [Module,ExtFunc,Args,Res]),
							ErrStr = lists:flatten(io_lib:format("~p",[Res])),
							encode_fault("ServiceException", "The called method threw an exception.", ErrStr) 
					end;
				{'EXIT', {'RequireHeaderException', Err}} ->
					writelog(Log, "Unsupported Header~n", []),
					encode_fault("RequireHeaderException", Err, "");
				{'EXIT', Err} ->
					ErrStr = lists:flatten(io_lib:format("~p",[Err])),
					writelog(Log, "Unrecognize request: ~s~n", [ErrStr]),
					encode_fault("ProtocolException", "The Hessian request has some sort of syntactic error.", ErrStr)
			end;
		[] ->
			writelog(Log, "Module not found, Url=~s~n", [Url]),
			encode_fault("NoSuchObjectException", "The requested object does not exist.", Url)
	end.

dispatch(Url, Bin) ->
	dispatch(Url, Bin, undefined).

