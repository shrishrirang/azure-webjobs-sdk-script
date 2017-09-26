@ECHO Off

SET Config=%1
IF "%1"=="" (
  SET Config="Release"
)

msbuild WebJobs.Script.sln /p:Configuration=%Config%;SolutionDir=%~dp0 /flp:LogFile=msbuild.log;Verbosity=Normal