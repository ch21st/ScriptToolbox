IF OBJECT_ID('SP_SYS_JobRunStatus') IS NULL EXEC('CREATE PROCEDURE SP_SYS_JobRunStatus AS RETURN 0')
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************
-- =============================================
-- Author: Nolan Shang
-- Create date: 06/06/2017
-- Description: 
--     Job run status checking
-- It is a private utility of Nolan,Support checking remoter server
-----------------------------------
-- =============================================
******************************************************************/
ALTER PROCEDURE [dbo].[SP_SYS_JobRunStatus]
   @RemoteServer VARCHAR(100)='',/*If keep blank will check local server*/
   @OnlyFailed BIT =0,/*0:Failed;1:All*/
   @Job_ID AS varchar(100)=NULL
AS
BEGIN

    SET NOCOUNT ON
    DECLARE @sql NVARCHAR(max),@Prefix VARCHAR(100)
    SET @RemoteServer=REPLACE(REPLACE(@RemoteServer,']',''),'[','')
    IF @RemoteServer IN ('','.','(Local)') SET @Prefix=''
    ELSE SET @Prefix='['+@RemoteServer+'].'

 --   BEGIN TRY
        IF @Job_ID IS NULL SET  @Job_ID = '%' /* you can specify a job id to query for a certain job*/ 

        IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results
        IF OBJECT_ID('tempdb..#JobResults') IS NOT NULL DROP TABLE #JobResults
        IF OBJECT_ID('tempdb..#JobList') IS NOT NULL DROP TABLE #JobList
        IF OBJECT_ID('tempdb..#RemoteResults') IS NOT NULL DROP TABLE #RemoteResults
        
         CREATE TABLE #Results(ID INT IDENTITY(1,1),job_index INT,job_id uniqueidentifier NOT NULL, job_name SYSNAME,step_name SYSNAME,step_id INT 
                               ,Job_Start_DateTime DATETIME,Job_Duration TIME,Current_Running_Step_ID INT ,job_run_Status INT ,step_run_status INT 
                               ,job_Run_Status_Description VARCHAR(1000),step_run_status_description VARCHAR(1000),Step_Start_DateTime DATETIME,Step_Duration TIME
         )        
        
        
        
        CREATE TABLE #JobResults(job_id uniqueidentifier NOT NULL, last_run_date int NOT NULL, 
                                 last_run_time int NOT NULL, next_run_date int NOT NULL, next_run_time int NOT NULL, next_run_schedule_id int NOT NULL, requested_to_run int NOT NULL, 
                                 /* bool*/ request_source int NOT NULL, request_source_id sysname COLLATE database_default NULL, running int NOT NULL, /* bool*/ current_step int NOT NULL, 
                                 current_retry_attempt int NOT NULL, job_state int NOT NULL
        ) 

         CREATE TABLE #JobList(ID INT IDENTITY(1,1),job_index INT,job_id uniqueidentifier NOT NULL, job_name SYSNAME
                              ,Job_Start_DateTime DATETIME,Job_Duration TIME,Current_Running_Step_ID INT ,job_run_Status INT 
                              ,job_Run_Status_Description VARCHAR(1000)
         )     
         
         
         CREATE TABLE #RemoteResults(job_id uniqueidentifier,Job_Name sysname,step_name sysname,step_id INT,last_run_outcome TINYINT
	                                ,step_run_status INT,Step_Run_Date int,Step_Run_Time int,Step_Duration int,Job_Start_DateTime datetime,Job_Duration time(7) 
         )    
        
     --    DECLARE @sql VARCHAR(max) ,@RemoteServer VARCHAR(100)='192.1.1.22'
        IF @RemoteServer IN ('','.','(Local)')
              SET @sql='EXEC master.dbo.xp_sqlagent_enum_jobs 1, '''''
        ELSE 
              SET @sql ='SELECT * FROM OPENQUERY([' + @RemoteServer + '], 
                        ''SET FMTONLY OFF set nocount on declare @jobid UNIQUEIDENTIFIER 
                        exec master.dbo.xp_sqlagent_enum_jobs 1, ''''sa'''', @jobid'')'

        PRINT @sql

        INSERT #JobResults 
        EXEC(@sql)

        PRINT '===============    Retrieve job run history   ============='
        SET @sql='SELECT job.job_id,job.name AS Job_Name,Jobstep.step_name,Jobstep.step_id,jobInfo.last_run_outcome,s.run_status AS step_run_status
                ,s.run_date as Step_Run_Date,s.run_time AS Step_Run_Time,s.run_duration as Step_Duration
                ,e.start_execution_date AS Job_Start_DateTime,cast(e.Duration AS time) AS Job_Duration
        FROM '+@Prefix+'msdb.dbo.sysjobs AS job --ON r.job_id = job.job_id
        INNER JOIN '+@Prefix+'msdb.dbo.sysjobsteps AS Jobstep ON Jobstep.job_id = job.job_id
        LEFT JOIN '+@Prefix+'msdb.dbo.sysjobservers AS jobInfo ON job.job_id = jobInfo.job_id 
        OUTER APPLY(
                SELECT TOP 1 start_execution_date,(ISNULL(stop_execution_date, GETDATE()) - start_execution_date) AS Duration
                FROM '+@Prefix+'[msdb].[dbo].[sysjobactivity]
                WHERE job_id = job.job_id ORDER BY start_execution_date DESC
        ) e
        OUTER APPLY(
                SELECT JobHistory.run_status,JobHistory.run_duration,JobHistory.run_date,JobHistory.run_time
			        FROM '+@Prefix+'msdb.dbo.sysjobhistory as JobHistory WITH (NOLOCK) 
			        WHERE job_id = job.job_id AND JobHistory.step_id=Jobstep.step_id and convert(datetime,STR(run_date) +'' ''+STUFF(STUFF(REPLACE(STR(run_time, 6, 0), '' '', ''0''), 3, 0, '':''), 6, 0, '':'') ) >= e.start_execution_date
        ) s
        WHERE EXISTS(SELECT 0 FROM '+@Prefix+'msdb.dbo.sysjobschedules  AS jobsch WHERE jobsch.job_id=job.job_id)'
        PRINT @sql
        INSERT INTO #RemoteResults  
        EXEC(@sql)


       

        INSERT INTO #Results(job_index,job_id,job_name,step_name,step_id,step_run_status,step_run_status_description,Step_Start_DateTime,Step_Duration
                             ,Job_Start_DateTime,Job_Duration,Current_Running_Step_ID,job_run_Status,job_Run_Status_Description)

        SELECT  DENSE_RANK() OVER (ORDER BY r.job_id) AS job_index
               ,r.job_id,rr.Job_Name,rr.step_name,rr.step_id
               ,CASE --convert to the uniform status numbers we are using
					        WHEN rr.step_run_status = 0 THEN 0
					        WHEN rr.step_run_status = 1 THEN 1
					        WHEN rr.step_run_status = 2 THEN 2
					        WHEN rr.step_run_status = 4 THEN 2
					        WHEN rr.step_run_status = 3 THEN 3
					        ELSE NULL 
				         END AS step_run_status
               ,CASE 
					        WHEN rr.step_run_status = 0 THEN 'Failed' 
					        WHEN rr.step_run_status = 1 THEN 'Success' 
					        WHEN rr.step_run_status = 2 THEN 'In Progress'
					        WHEN rr.step_run_status = 4 THEN 'In Progress' 
					        WHEN rr.step_run_status = 3 THEN 'Canceled' 
					        ELSE 'Unknown' 
				         END AS step_run_status_description
               ,CAST(STR(rr.Step_Run_Date) AS DATETIME) + CAST(STUFF(STUFF(REPLACE(STR(rr.Step_Run_Time, 6, 0), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS TIME) as Step_Start_DateTime
  			   ,CAST(CAST(STUFF(STUFF(REPLACE(STR(rr.Step_Duration % 240000, 6, 0), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME) AS TIME) AS Step_Duration
               ,rr.Job_Start_DateTime,cast(rr.Job_Duration AS time) AS Job_Duration
               ,r.current_step AS Current_Running_Step_ID
               ,CASE WHEN r.running = 0 THEN rr.last_run_outcome ELSE /*convert to the uniform status numbers (my design)*/ 
                      CASE WHEN r.job_state = 0 THEN 1 /*success*/ 
                           WHEN r.job_state = 4 THEN 1 
                           WHEN r.job_state = 5 THEN 1 
                           WHEN r.job_state = 1 THEN 2 /*in progress*/ 
                           WHEN r.job_state = 2 THEN 2 
                           WHEN r.job_state = 3 THEN 2 
                           WHEN r.job_state = 7 THEN 2 
                      END 
                 END AS Run_Status
                ,CASE WHEN r.running = 0 THEN /* sysjobservers will give last run status, but does not know about current running jobs*/ 
                           CASE WHEN rr.last_run_outcome= 0 THEN 'Failed' 
                                WHEN rr.last_run_outcome = 1 THEN 'Success' 
                                WHEN rr.last_run_outcome = 3 THEN 'Canceled' ELSE 'Unknown' 
                           END /* succeeded, failed or was canceled.*/ 
                       WHEN r.job_state = 0 THEN 'Success' 
                       WHEN r.job_state = 4 THEN 'Success' 
                       WHEN r.job_state = 5 THEN 'Success' 
                       WHEN r.job_state = 1 THEN 'In Progress' 
                       WHEN r.job_state = 2 THEN 'In Progress' 
                       WHEN r.job_state = 3 THEN 'In Progress' 
                       WHEN r.job_state = 7 THEN 'In Progress' 
                       ELSE 'Unknown' 
                 END AS Run_Status_Description
        FROM #JobResults AS r
        INNER JOIN #RemoteResults AS rr ON rr.job_id=r.job_id



        INSERT INTO #JobList (job_index,job_id,job_name,Job_Start_DateTime,Job_Duration,Current_Running_Step_ID,job_run_Status,job_Run_Status_Description )
        SELECT DISTINCT j.job_index,j.job_id,j.job_name,j.Job_Start_DateTime,j.Job_Duration,j.Current_Running_Step_ID,j.job_run_Status,j.job_Run_Status_Description   
        FROM #Results AS j

        
        SELECT o.job_id, o.job_name,o.Job_Start_DateTime,o.Job_Duration,o.job_run_Status,o.job_Run_Status_Description,o.Current_Running_Step_ID
              ,r.step_id,r.step_name,r.Step_Start_DateTime,r.Step_Duration,r.step_run_status,r.step_run_status_description
        FROM #JobList AS o INNER JOIN #Results AS r ON r.job_index=o.job_index 
      
        WHERE o.job_run_Status=CASE WHEN @OnlyFailed=1 THEN 0 ELSE o.job_run_Status END 



            --  SET @tableHTML=REPLACE(REPLACE(@tableHTML,'&lt;','<'),'&gt;','>')
        IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results
        IF OBJECT_ID('tempdb..#JobResults') IS NOT NULL DROP TABLE #JobResults
        IF OBJECT_ID('tempdb..#JobList') IS NOT NULL DROP TABLE #JobList
        IF OBJECT_ID('tempdb..#RemoteResults') IS NOT NULL DROP TABLE #RemoteResults

   /*         
	END TRY
	BEGIN CATCH
   		    PRINT ERROR_MESSAGE()
		    RETURN -999

	END CATCH
    */
	SET NOCOUNT Off
END

/*

EXEC dbo.SP_SYS_JobRunStatus  @OnlyFailed=1

  
*/

EXEC dbo.SP_SYS_JobRunStatus  @OnlyFailed=0
