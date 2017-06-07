-module(two_phase_commit).
-compile(export_all).

%API
initializeCommit(Coordinator) ->
  Coordinator ! {self(),{initialize_commit}}.

startCoordinator() ->
  % starts coordinator, which administrates message to and from cohorts
  P = spawn(fun () -> coordinator([],[],[], make_ref()) end),
  P.

startCohort(State,Server,Fun) ->
  % start cohort with an empty state and a given function
  C = spawn(fun () -> cohort(State,Fun,null,make_ref()) end),
  connect(Server,C).

%Coordinator server
coordinator(Cohorts, Decided, Ack, Ref) ->
  receive
    % handle cohort connection
    {_, {connect_to_coord,Cohort}} ->
      io:format("Coordinator: Cohort has connected ~n"),
      Cohort ! {self(),{ok, Ref}},
      coordinator([Cohort | Cohorts],Decided, Ack, Ref);
    % initialize commit by broadcasting commit messages to all cohorts
    {_,{initialize_commit}} ->
      io:format("Coordinator: Received commit request ~n"),
      broadcastCohorts(Cohorts,self(),{Ref,can_commit}),
      coordinator(Cohorts,Decided, Ack, Ref);
    % handle commit decision messages
    {From,{can_commit, Decision}} ->
     io:format("Coordinator: Received commit response ~n"),
      case Decision of
        false ->
          broadcastCohorts(Cohorts,self(),{commit_desc,Ref,abort}),
          coordinator(Cohorts,[], Ack, Ref);
        true ->
          % add cohort to list of decided cohorts, check if all have commited
          % broadcast commit message if possible
          NewDecided = [From | Decided],
          case length(NewDecided) =:= length(Cohorts) of
            true ->
              broadcastCohorts(Cohorts,self(),{commit_desc,Ref,commit}),
              coordinator(Cohorts,NewDecided, Ack, Ref);
            false ->
              coordinator(Cohorts,NewDecided, Ack, Ref)
          end
      end;
    % receive message once commit is done/aborted
    % reset state once all have decided
    {From,{acknowledge,Desc}} ->
      case len([Desc | Ack]) =:= len(Cohorts) of
        true  ->
          io:format("Coordinator: command done: decision was: ~s ~n", [Desc]),
          coordinator([],[],[], Ref);
        false ->
          coordinator(Cohorts,Decided, [Desc | Ack], Ref)
      end
  end.

% Cohort server
cohort(State,Action,C,C_ref) ->
  receive
    %TODO ref instead
    {_,{ok,Coord}} ->
      cohort(State,Action,Coord,C_ref);
    {From,{Coord,can_commit}} ->
      io:format("Cohort ~p: Received commmit request: ~n", [C_ref]),
      PrevState = State,
      {Desc,Newstate} = runAction(Action,State),
      case Desc of
        success -> From ! {self(),{can_commit,true}};
        failure -> From ! {self(),{can_commit,false}}
      end,
      receive
        {From,{commit_desc,Coord,CommitDecision}} ->
          io:format("Cohort ~p: Received commmmit decision ~n", [C_ref]),
          case CommitDecision of
            commit ->
              %Newstate = Action(State),
              From ! {self(),{acknowledge,commit}},
              io:format("Cohort ~p: State is: ~p ~n", [C_ref,Newstate]),
              cohort(State,Newstate,Coord,C_ref);
            abort ->
              From ! {self(),{acknowledge,abort}},
              io:format("Cohort ~p: state is: Aborted ~n", [C_ref]),
              cohort(Newstate,Action,Coord,C_ref)
          end
        %TODO: Keep stored trans if cohort dies
        after 100 ->
          From ! {self(), {get_decision}},
          receive
            {From, {get_decision, yes}} ->
              cohort(Newstate,Action,Coord,C_ref);
            {From, {get_decision, no}}  ->
              cohort(Newstate,Action,Coord,C_ref)
            after 100 ->
              % Server is dead, revert to previous state, loop again to await
              % further instructions from replaced coordinator (not impl)
              cohort(PrevState,Action,Coord,C_ref)
          end
      end
  end.

% Helper functions
broadcastCohorts(Cohorts,Server,Message) ->
  lists:foreach(fun(PID) -> PID ! {Server,Message} end, Cohorts).

connect(Coord,Cohort) ->
  Coord ! {self(),{connect_to_coord,Cohort}}.

len([]) -> 0;
len([_|T]) -> 1 + len(T).

runAction(Action,State) ->
  try Action(State) of
    _ -> {success,Action(State)}
  catch
    _:_ ->
      {failure,State}
  end.