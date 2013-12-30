-module(parser_funcs).
-include("parser.hrl").
-import(string,[tokens/2,str/2,substr/2,substr/3,len/1,concat/2,to_integer/1,to_float/1]).
-import(lists,[reverse/1]).
-import(re,[split/3,run/2]).
-import(file,[open/2,close/1,pread/3]).
-export([parse_string/1,parse_line/1,read_file/2,parse_message/0,parse_file/3]).

parse_file(FileName,Start,End) when End-Start>1000000 ->
	spawn(parser_funcs,read_file,[FileName,Start]),
	parse_file(FileName,Start+1000000,End);
parse_file(FileName,Start,End)->
	spawn(parser_funcs,read_file,[FileName,Start]).

read_file(FileName,LastPosition)->
	{ok,File}=open(FileName,[read]),
	{ok,Data}=pread(File,LastPosition,1000000),
	DataStrings=split(Data,"\n",[{return,list}]),
	StopAt=parse_data(FileName,0,DataStrings),
	close(File),
	StopAt.

parse_data(FileName,LastPosition,[])->LastPosition;
parse_data(FileName,LastPosition,[String|[]])->LastPosition;
parse_data(FileName,LastPosition,[String|Strings])->
%	io:format("String ~p ~n",[String]),
%	io:format("LastPos: ~p ~n",[LastPosition]),
%	parse_line(String),
%	Pid = self(),
%	Pid ! String,
	Pid = spawn(parser_funcs,parse_message,[]),
	Pid ! {log_string,String},
	parse_data(FileName,LastPosition+len(String)+1,Strings).

parse_message()->
	receive
		{log_string,String}->parse_line(String);
		_Other -> {error}
	end.

parse_line(Str)->
	PreParsed = parse_string(Str),
	Upstream = split(PreParsed#parsed_line.upstream," : ",[{return,list}]),
	UpstreamTime = split(PreParsed#parsed_line.upstreamtime," : ",[{return,list}]),
	UpstreamCode = split(PreParsed#parsed_line.upstreamcode," : ",[{return,list}]),
	send_to_next(PreParsed,Upstream,UpstreamTime,UpstreamCode)
	.

send_to_next(_Parsed,[],[],[])->[];
send_to_next(Parsed,[HUp|Upstream],[HUpTime|UpstreamTime],[HUpCode|UpstreamCode])->
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
%	io:format("~p ~n",[P1]),
	send_to_next(Parsed,Upstream,UpstreamTime,UpstreamCode).

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
