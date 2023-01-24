-module(consulate_client).

-include_lib("kernel/include/logger.hrl").

-compile({no_auto_import, [get/1, put/2]}).

-export([register/2,
         deregister/1,
         list_local_services/0,
         list_services/1,
         get_local_service/1,
         get_service/2]).

register(Name, Port) ->
    CheckOpts = application:get_env(consulate, check, #{}),
    Interval = maps:get(interval, CheckOpts, 10),
    Deregister = maps:get(deregister, CheckOpts, 60),
    Meta = application:get_env(consulate, meta, #{}),
    Service = #{
      'ID' => Name,
      'Name' => service_name(),
      'Port' => Port,
      'Meta' => Meta,
      'Check' => #{
        'Name' => <<"Check Erlang Distribuition Port">>,
        'Interval' => format("~Bs", [Interval]),
        'DeregisterCriticalServiceAfter' => format("~Bs", [Deregister]),
        'TCP' => format("localhost:~B", [Port]),
        'Status' => <<"passing">>
       },
      'EnableTagOverride' => false
     },
    put("/v1/agent/service/register", Service).

deregister(Name) ->
    Endpoint = io_lib:format("/v1/agent/service/deregister/~s", [Name]),
    put(Endpoint, #{}).

list_local_services() ->
    get("/v1/agent/services").

list_services(Host) ->
    Path = io_lib:format("/v1/catalog/service/~s", [service_name()]),
    Filter = io_lib:format("Node == \"~s\"", [Host]),
    Endpoint = {Path, [{"filter", Filter}]},
    get(Endpoint).

get_local_service(Name) ->
    logger:set_application_level(consulate_client, debug),
    logger:set_application_level(default, debug),
    ?LOG_DEBUG("consulate:get_local_service(~p)", [Name]),
    Endpoint = io_lib:format("/v1/agent/service/~s", [Name]),
    get(Endpoint).

get_service(Name, Host) ->
    Path = io_lib:format("/v1/catalog/service/~s", [service_name()]),
    Filter = io_lib:format("ServiceID == \"~s\" and Node == \"~s\"", [Name, Host]),
    Endpoint = {Path, [{"filter", Filter}]},
    get(Endpoint).

get(Endpoint) ->
    Url = build_url(Endpoint),
    ?LOG_DEBUG("consulate:get(~p) -> ~p", [Endpoint, Url]),
    case hackney:request(get, Url, [], [], []) of 
        {ok, 200, _, ClientRef} ->
            {ok, Body} = hackney:body(ClientRef),
            {ok, jsx:decode(Body, [return_maps])};
        {error, Reason} ->
            {error, Reason}
    end.

put(Endpoint, Message) ->
    Body = jsx:encode(Message),
    Url = build_url(Endpoint),

    ?LOG_DEBUG("consulate:put(~p, ~p) -> ~p @ ~p", [Endpoint, Message, Url, Body]),

    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    Payload = Body,
    Options = [],
    case hackney:request(put, Url, Headers, Payload, Options) of 
        {ok, 200, _, _} -> ok;
        {error, _Response} -> error
    end.

build_url({Path, Query}) ->
    URI = #{
      host => application:get_env(consulate, host, "127.0.0.1"),
      path => Path,
      port => application:get_env(consulate, port, 8500),
      scheme => application:get_env(consulate, scheme, "http"),
      query => uri_string:compose_query(Query)
     },
    uri_string:recompose(URI);
build_url(Path) ->
    build_url({Path, []}).

service_name() ->
    case init:get_argument(consul_service) of
        {ok, [Name]} -> list_to_binary(Name);
        _ -> <<"erlang-service">>
    end.

format(Template, Args) ->
    list_to_binary(io_lib:format(Template, Args)).
