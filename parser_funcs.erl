-module(parser_funcs).
-include("parser.hrl").
-include_lib("kernel/include/file.hrl").
-import(string,[tokens/2,str/2,substr/2,substr/3,len/1,concat/2,to_integer/1,to_float/1]).
-import(lists,[reverse/1]).
-import(re,[split/3,run/2]).
-import(file,[open/2,close/1,pread/3,list_dir/1,read_file_info/1]).
-import(random,[uniform/0,uniform/1,seed/3,seed/0]).
-import(timer,[sleep/1]).
-import(ets,[new/2,insert/2,lookup/2]).
-export([parse_string/1,parse_line/3,read_file/4,parse_file/4,parse/3,main/0,readdir/2,process_dir/2]).

main()->
	application:start(mongodb),
	application:start(bson),
	ets:new(filesizes,[set,public,named_table]),
	ConnPool=resource_pool:new(mongo:connect_factory({"localhost",27017}),200),
%	check_dirs(ConnPool,["/home/woyager/work/erl_parser/nginx2/","/home/woyager/work/erl_parser/nginx4/","/home/woyager/work/erl_parser/nginx5/","/home/woyager/work/erl_parser/nginx6/"])
	check_dirs(ConnPool,["/mnt/nfs-logs/nginx2/","/mnt/nfs-logs/nginx4/","/mnt/nfs-logs/nginx5/","/mnt/nfs-logs/nginx6/"])
.

check_dirs(ConnPool,[])->started_all;
check_dirs(ConnPool,[Dir|Dirs])->
	spawn(parser_funcs,readdir,[ConnPool,Dir]),
%	readdir(ConnPool,Dir),
	check_dirs(ConnPool,Dirs).

readdir(ConnPool,Dir)->
	io:format("started ~p ~n",[Dir]),
%	spawn(parser_funcs,process_dir,[ConnPool,Dir]),
	process_dir(ConnPool,Dir),
	sleep(30000),
	readdir(ConnPool,Dir).

process_dir(ConnPool,Dir) -> 
	{ok,Filenames}=list_dir(Dir),
	filt_files(ConnPool,Dir,Filenames)
	.

filt_files(ConnPool,Dir,[])-> ok;
filt_files(ConnPool,Dir,[Filename|Filenames])->
	case run(Filename,"(.*access\.log\.1)$") of
		{match,Match}->process_file(ConnPool,Dir,Filename);
		_ -> not_match
	end,
	filt_files(ConnPool,Dir,Filenames)
	.

process_file(ConnPool,Dir,Filename)->
	File = concat(Dir,Filename),
	{ok,Fileinfo}=read_file_info(File),
	FF = ets:lookup(filesizes,File),
	Size = Fileinfo#file_info.size,
	case FF of
		[] ->	io:format("New file found ~p ~n",[File]),
			ets:insert(filesizes,{File,Size});
		[{_,OldSize}] -> 
			if OldSize > Size ->
				io:format("Process from 0 ~p ~n",[File]),
				ets:insert(filesizes,{File,Size}),
				parse_file(File,0,Size,ConnPool);
			   OldSize =< Size ->
				io:format("Process from ~p ~p ~n",[OldSize,File]),
				ets:insert(filesizes,{File,Size}),
				parse_file(File,OldSize,Size,ConnPool)
			end;
		_ -> not_found
	end
	.

parse(FileName,Start,End)->
	application:start(mongodb),
	application:start(bson),
	ConnPool=resource_pool:new(mongo:connect_factory({"localhost",27017}),50),
	parse_file(FileName,Start,End,ConnPool)
	.

parse_file(FileName,Start,End,ConnPool) when End-Start>1000000 ->
	spawn(parser_funcs,read_file,[FileName,Start,1000000,ConnPool]),
	parse_file(FileName,Start+1000000,End,ConnPool);
parse_file(FileName,Start,End,ConnPool)->
	spawn(parser_funcs,read_file,[FileName,Start,End-Start,ConnPool]).

read_file(FileName,LastPosition,Len,ConnPool)->
	{ok,File}=open(FileName,[read]),
	{ok,Data}=pread(File,LastPosition,Len),
	{ok,Conn}=resource_pool:get(ConnPool),
	DataStrings=split(Data,"\n",[{return,list}]),
	StopAt=parse_data(FileName,0,DataStrings,Conn),
	close(File),
	StopAt.

parse_data(FileName,LastPosition,[],Mongo)->LastPosition;
parse_data(FileName,LastPosition,[String|[]],Mongo)->LastPosition;
parse_data(FileName,LastPosition,[String|Strings],Mongo)->
%	io:format("String ~p ~n",[String]),
%	io:format("LastPos: ~p ~n",[LastPosition]),
%	parse_line(String,Mongo),
%	Pid = self(),
%	Pid ! String,
	Test2 = run(FileName,"nginx2"),
	Test4 = run(FileName,"nginx4"),
	Test5 = run(FileName,"nginx5"),
	Test6 = run(FileName,"nginx6"),
	case Test2 of
		{match,_} -> spawn(parser_funcs,parse_line,[String,Mongo,"nginx2"]);
		_ -> next 
	end,
	case Test4 of
		{match,_} -> spawn(parser_funcs,parse_line,[String,Mongo,"nginx4"]);
		_ -> next
	end,
	case Test5 of
		{match,_} -> spawn(parser_funcs,parse_line,[String,Mongo,"nginx5"]);
		_ -> next
	end,
	case Test6 of
		{match,_} -> spawn(parser_funcs,parse_line,[String,Mongo,"nginx6"]);
		_ -> next
	end,
%	parse_line(String,Mongo),
	parse_data(FileName,LastPosition+len(String)+1,Strings,Mongo).

parse_line(Str,Mongo,Frontend)->
	PreParsed = parse_string(Str),
	Upstream = split(PreParsed#parsed_line.upstream," : ",[{return,list}]),
	UpstreamTime = split(PreParsed#parsed_line.upstreamtime," : ",[{return,list}]),
	UpstreamCode = split(PreParsed#parsed_line.upstreamcode," : ",[{return,list}]),
	send_to_next(PreParsed#parsed_line{frontend=Frontend},Upstream,UpstreamTime,UpstreamCode,Mongo)
	.

send_to_next(_Parsed,[],[],[],Mongo)->[];
send_to_next(Parsed,[HUp|Upstream],[HUpTime|UpstreamTime],[HUpCode|UpstreamCode],Mongo)->
	{CodeT,_}=to_integer(HUpCode),
	{TimeT,_}=to_float(HUpTime),
	Code = case CodeT of
		error -> -1;
		_ -> CodeT
		end,
	Time = case TimeT of
		error -> -0.001;
		_ -> TimeT
		end,
	P1 = Parsed#parsed_line{upstream=HUp,upstreamtime=Time*1000,upstreamcode=Code,resptime=Parsed#parsed_line.resptime*1000},

% ip,user,date_time_tz,method,url,version,code,size,ref,agent,resp_time,upstream_time,upstream,cache_status,upstream_code,memcache_key,enhanced_memcache_key,backend,resp_compl,content_type,request_id,uid_got,uid_set,farm,country,real_ip,cdn_client_ip,http_ip,live_ip,date_time,tstamp,frontend,host,gf

	{ok,_}=mongo:do(safe,master,Mongo,logs2,fun()-> mongo:insert(lines,
			{
				ip,bson:utf8(P1#parsed_line.ip),
				user,bson:utf8(P1#parsed_line.user),
				date_time_tz,bson:utf8(P1#parsed_line.datetimetz),
				method,bson:utf8(P1#parsed_line.method),
				url,bson:utf8(P1#parsed_line.url),
				version,bson:utf8(P1#parsed_line.version),
				code,P1#parsed_line.code,
				size,P1#parsed_line.size,
				ref,bson:utf8(P1#parsed_line.ref),
				agent,bson:utf8(P1#parsed_line.agent),
				resp_time,P1#parsed_line.resptime,
				upstream_time,P1#parsed_line.upstreamtime,
				upstream,bson:utf8(P1#parsed_line.upstream),
				cache_status,bson:utf8(P1#parsed_line.cachestatus),

				upstream_code,P1#parsed_line.upstreamcode,
				memcache_key,bson:utf8(P1#parsed_line.memcachekey),
				enhanced_memcache_key,bson:utf8(P1#parsed_line.enhancedmemcachekey),
				backend,bson:utf8(P1#parsed_line.backend),
				resp_compl,bson:utf8(P1#parsed_line.respcompl),
				content_type,bson:utf8(P1#parsed_line.contenttype),
				request_id,bson:utf8(P1#parsed_line.requestid),
				uid_got,bson:utf8(P1#parsed_line.uidgot),
				uid_set,bson:utf8(P1#parsed_line.uidset),
				farm,bson:utf8(P1#parsed_line.farm),
				country,bson:utf8(P1#parsed_line.country),
				real_ip,bson:utf8(P1#parsed_line.realip),
				cdn_client_ip,bson:utf8(P1#parsed_line.cdnclientip),
				http_ip,bson:utf8(P1#parsed_line.httpip),
				live_ip,bson:utf8(P1#parsed_line.liveip),
				date,P1#parsed_line.date,
				month,P1#parsed_line.month,
				year,P1#parsed_line.year,
				hour,P1#parsed_line.hour,
				min,P1#parsed_line.minutes,
				seconds,P1#parsed_line.seconds,
				tstamp,P1#parsed_line.tstamp,
				frontend,bson:utf8(P1#parsed_line.frontend),
				host,bson:utf8(P1#parsed_line.host),
				gf,bson:utf8(lists:concat([P1#parsed_line.upstream,"    ",P1#parsed_line.code,"    ",P1#parsed_line.host,"    ",P1#parsed_line.backend,"    ",P1#parsed_line.frontend,"    ",P1#parsed_line.cachestatus,"    ",P1#parsed_line.contenttype,"    ",P1#parsed_line.url,"    ",P1#parsed_line.upstreamcode,"    ",P1#parsed_line.farm,"    ",P1#parsed_line.country]))
			}
		) end),
	send_to_next(Parsed,Upstream,UpstreamTime,UpstreamCode,Mongo).

parse_string(Str)->
	Items = split(concat(Str,"end"),"    ",[{return,list}]),
	[Ip,User,DateTimeTZ,Method,Url,Version,CodeStr,SizeStr,Ref,Agent,RespTimeStr,UpstreamTime,Upstream,CacheStatus,UpstreamCode,MemcacheKey,EnhancedMemcacheKey,Backend,RespCompl,ContentType,RequestId,UidGot,UidSet,Farm,Country,RealIp,CdnClientIp,HttpIp,LiveIp,Hz]=Items,
	{CodeT,_}=to_integer(CodeStr),
	{SizeT,_}=to_integer(SizeStr),
	{RespTimeT,_}=to_float(RespTimeStr),
	Code = case CodeT of
		error -> -1;
		_ -> CodeT
	       end,
	Size = case SizeT of
		error -> -1;
		_ -> SizeT
	       end,
	RespTime = case RespTimeT of
			error -> -0.001;
			_ -> RespTimeT
		   end,
		
	{match,[_,{DateST,DateSE},{MonST,MonSE},{YearST,YearSE},{HourST,HourSE},{MinST,MinSE},{SecST,SecSE}]}=run(DateTimeTZ,"(..)/(...)/(....):(..):(..):(..).*"),
	{Date,_}=to_integer(substr(DateTimeTZ,DateST+1,DateSE)),
	Mon=monthToNum(substr(DateTimeTZ,MonST+1,MonSE)),
	{Year,_}=to_integer(substr(DateTimeTZ,YearST+1,YearSE)),
	{Hour,_}=to_integer(substr(DateTimeTZ,HourST+1,HourSE)),
	{Min,_}=to_integer(substr(DateTimeTZ,MinST+1,MinSE)),
	{Sec,_}=to_integer(substr(DateTimeTZ,SecST+1,SecSE)),
	Tstamp=calendar:datetime_to_gregorian_seconds({{Year,Mon,Date},{Hour,Min,Sec}})-62167219200,
	{match,[_,{HostSt,HostSe}]}=run(Url,"http:\/\/(.*?)\/.*$"),
	Host=substr(Url,HostSt+1,HostSe),
	#parsed_line{
		ip=Ip,
		user=User,
		datetimetz=DateTimeTZ,
		date=Date,
		month=Mon,
		year=Year,
		hour=Hour,
		minutes=Min,
		seconds=Sec,
		tstamp=Tstamp,
		method=Method,
		host=Host,
		url=Url,
		version=Version,
		code=Code,
		size=Size,
		ref=Ref,
		agent=Agent,
		resptime=RespTime,
		upstreamtime=UpstreamTime,
		upstream=Upstream,
		cachestatus=CacheStatus,
		upstreamcode=UpstreamCode,
		memcachekey=MemcacheKey,
		enhancedmemcachekey=EnhancedMemcacheKey,
		backend=Backend,
		respcompl=RespCompl,
		contenttype=ContentType,
		requestid=RequestId,
		uidgot=UidGot,
		uidset=UidSet,
		farm=Farm,
		country=Country,
		realip=RealIp,
		cdnclientip=CdnClientIp,
		httpip=HttpIp,
		liveip=LiveIp,
		hz=Hz
	}.

monthToNum("Jan") -> 1;
monthToNum("Feb") -> 2;
monthToNum("Mar") -> 3;
monthToNum("Apr") -> 4;
monthToNum("May") -> 5;
monthToNum("Jun") -> 6;
monthToNum("Jul") -> 7;
monthToNum("Aug") -> 8;
monthToNum("Sep") -> 9;
monthToNum("Oct") -> 10;
monthToNum("Nov") -> 11;
monthToNum(_) -> 12.
