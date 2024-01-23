using FluentAssertions;
using Quartz;
using Quartz.Impl;
using Quartz.Impl.AdoJobStore;
using RootNine.Persistence;
using System.Collections.Specialized;
using Xunit;

namespace SchedulerErrorTest
{
    public class TestClass
    {

        [Fact]
        public async Task ExecuteWithDatabase()
        {
            using var connectionFactory = new LocalDbConnectionFactory();
            await InitDatabase(connectionFactory);

            var schedulerFactory = new StdSchedulerFactory(GetProperties(connectionFactory));
            var scheduler = await schedulerFactory.GetScheduler();

            try
            {
                await scheduler.Start();

                var jobKey = new JobKey("test");
                var job = JobBuilder.Create<ExecuterJob>()
                    .WithIdentity(jobKey)
                    .RequestRecovery(true)
                    .StoreDurably(false)
                    .Build();
                var trigger = TriggerBuilder.Create()
                    .StartNow()
                    .Build();

                ExecuterJob.Running.Should().Be(0);
                await scheduler.ScheduleJob(job, trigger);


                await ExecuterJob.WaitTillRunning();
                ExecuterJob.Running.Should().Be(1);

                var triggers = await scheduler.GetTriggersOfJob(jobKey);
                triggers.Should().HaveCount(1);
                var triggerKey = triggers.Single().Key;
                var triggerState = await scheduler.GetTriggerState(triggerKey);
                triggerState.Should().NotBe(TriggerState.Complete);

                ExecuterJob.ContinueRunning();
                SpinWait.SpinUntil(() => ExecuterJob.Running == 0);
            }
            finally
            {
                await scheduler.Shutdown();
            }
        }

        [Fact]
        public async Task ExecuteWithRamStorage()
        {
            var schedulerFactory = new StdSchedulerFactory(GetProperties(null));
            var scheduler = await schedulerFactory.GetScheduler();

            try
            {
                await scheduler.Start();

                var jobKey = new JobKey("test");
                var job = JobBuilder.Create<ExecuterJob>()
                    .WithIdentity(jobKey)
                    .RequestRecovery(true)
                    .StoreDurably(false)
                    .Build();
                var trigger = TriggerBuilder.Create()
                    .StartNow()
                    .Build();

                ExecuterJob.Running.Should().Be(0);
                await scheduler.ScheduleJob(job, trigger);


                await ExecuterJob.WaitTillRunning();
                ExecuterJob.Running.Should().Be(1);

                var triggers = await scheduler.GetTriggersOfJob(jobKey);
                triggers.Should().HaveCount(1);
                var triggerKey = triggers.Single().Key;
                var triggerState = await scheduler.GetTriggerState(triggerKey);
                triggerState.Should().NotBe(TriggerState.Complete);

                ExecuterJob.ContinueRunning();
                SpinWait.SpinUntil(() => ExecuterJob.Running == 0);
            }
            finally
            {
                await scheduler.Shutdown();
            }
        }

        private async Task InitDatabase(LocalDbConnectionFactory connectionFactory)
        {
            string[] commands = [
            @"CREATE TABLE [dbo].[QRTZ_CALENDARS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [CALENDAR_NAME] nvarchar(200) NOT NULL,
              [CALENDAR] varbinary(max) NOT NULL
            );",
            @"CREATE TABLE [dbo].[QRTZ_CRON_TRIGGERS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [TRIGGER_NAME] nvarchar(150) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL,
              [CRON_EXPRESSION] nvarchar(120) NOT NULL,
              [TIME_ZONE_ID] nvarchar(80) 
            );",
            @"CREATE TABLE [dbo].[QRTZ_FIRED_TRIGGERS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [ENTRY_ID] nvarchar(140) NOT NULL,
              [TRIGGER_NAME] nvarchar(150) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL,
              [INSTANCE_NAME] nvarchar(200) NOT NULL,
              [FIRED_TIME] bigint NOT NULL,
              [SCHED_TIME] bigint NOT NULL,
              [PRIORITY] int NOT NULL,
              [STATE] nvarchar(16) NOT NULL,
              [JOB_NAME] nvarchar(150) NULL,
              [JOB_GROUP] nvarchar(150) NULL,
              [IS_NONCONCURRENT] bit NULL,
              [REQUESTS_RECOVERY] bit NULL 
            );",
            @"CREATE TABLE [dbo].[QRTZ_PAUSED_TRIGGER_GRPS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL 
            );",
            @"CREATE TABLE [dbo].[QRTZ_SCHEDULER_STATE] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [INSTANCE_NAME] nvarchar(200) NOT NULL,
              [LAST_CHECKIN_TIME] bigint NOT NULL,
              [CHECKIN_INTERVAL] bigint NOT NULL
            );",
            @"CREATE TABLE [dbo].[QRTZ_LOCKS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [LOCK_NAME] nvarchar(40) NOT NULL 
            );",
            @"CREATE TABLE [dbo].[QRTZ_JOB_DETAILS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [JOB_NAME] nvarchar(150) NOT NULL,
              [JOB_GROUP] nvarchar(150) NOT NULL,
              [DESCRIPTION] nvarchar(250) NULL,
              [JOB_CLASS_NAME] nvarchar(250) NOT NULL,
              [IS_DURABLE] bit NOT NULL,
              [IS_NONCONCURRENT] bit NOT NULL,
              [IS_UPDATE_DATA] bit NOT NULL,
              [REQUESTS_RECOVERY] bit NOT NULL,
              [JOB_DATA] varbinary(max) NULL
            );",
            @"CREATE TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS] (
                [SCHED_NAME] nvarchar(120) NOT NULL,
                [TRIGGER_NAME] nvarchar(150) NOT NULL,
                [TRIGGER_GROUP] nvarchar(150) NOT NULL,
                [REPEAT_COUNT] int NOT NULL,
                [REPEAT_INTERVAL] bigint NOT NULL,
                [TIMES_TRIGGERED] int NOT NULL
            );",
            @"CREATE TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [TRIGGER_NAME] nvarchar(150) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL,
              [STR_PROP_1] nvarchar(512) NULL,
              [STR_PROP_2] nvarchar(512) NULL,
              [STR_PROP_3] nvarchar(512) NULL,
              [INT_PROP_1] int NULL,
              [INT_PROP_2] int NULL,
              [LONG_PROP_1] bigint NULL,
              [LONG_PROP_2] bigint NULL,
              [DEC_PROP_1] numeric(13,4) NULL,
              [DEC_PROP_2] numeric(13,4) NULL,
              [BOOL_PROP_1] bit NULL,
              [BOOL_PROP_2] bit NULL,
              [TIME_ZONE_ID] nvarchar(80) NULL 
            );",
            @"CREATE TABLE [dbo].[QRTZ_BLOB_TRIGGERS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [TRIGGER_NAME] nvarchar(150) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL,
              [BLOB_DATA] varbinary(max) NULL
            );",
            @"CREATE TABLE [dbo].[QRTZ_TRIGGERS] (
              [SCHED_NAME] nvarchar(120) NOT NULL,
              [TRIGGER_NAME] nvarchar(150) NOT NULL,
              [TRIGGER_GROUP] nvarchar(150) NOT NULL,
              [JOB_NAME] nvarchar(150) NOT NULL,
              [JOB_GROUP] nvarchar(150) NOT NULL,
              [DESCRIPTION] nvarchar(250) NULL,
              [NEXT_FIRE_TIME] bigint NULL,
              [PREV_FIRE_TIME] bigint NULL,
              [PRIORITY] int NULL,
              [TRIGGER_STATE] nvarchar(16) NOT NULL,
              [TRIGGER_TYPE] nvarchar(8) NOT NULL,
              [START_TIME] bigint NOT NULL,
              [END_TIME] bigint NULL,
              [CALENDAR_NAME] nvarchar(200) NULL,
              [MISFIRE_INSTR] int NULL,
              [JOB_DATA] varbinary(max) NULL
            );",
            @"ALTER TABLE [dbo].[QRTZ_CALENDARS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_CALENDARS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [CALENDAR_NAME]
              );",
            @"ALTER TABLE [dbo].[QRTZ_CRON_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_CRON_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_FIRED_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_FIRED_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [ENTRY_ID]
              );",
            @"ALTER TABLE [dbo].[QRTZ_PAUSED_TRIGGER_GRPS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_PAUSED_TRIGGER_GRPS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_SCHEDULER_STATE] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_SCHEDULER_STATE] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [INSTANCE_NAME]
              );",
            @"ALTER TABLE [dbo].[QRTZ_LOCKS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_LOCKS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [LOCK_NAME]
              );",
            @"ALTER TABLE [dbo].[QRTZ_JOB_DETAILS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_JOB_DETAILS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [JOB_NAME],
                [JOB_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_SIMPLE_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_SIMPROP_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_BLOB_TRIGGERS] WITH NOCHECK ADD
              CONSTRAINT [PK_QRTZ_BLOB_TRIGGERS] PRIMARY KEY  CLUSTERED
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              );",
            @"ALTER TABLE [dbo].[QRTZ_CRON_TRIGGERS] ADD
              CONSTRAINT [FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) ON DELETE CASCADE;",
            @"ALTER TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS] ADD
              CONSTRAINT [FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) ON DELETE CASCADE;",
            @"ALTER TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS] ADD
              CONSTRAINT [FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
              (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
                [SCHED_NAME],
                [TRIGGER_NAME],
                [TRIGGER_GROUP]
              ) ON DELETE CASCADE;",
            @"ALTER TABLE [dbo].[QRTZ_TRIGGERS] ADD
              CONSTRAINT [FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS] FOREIGN KEY
              (
                [SCHED_NAME],
                [JOB_NAME],
                [JOB_GROUP]
              ) REFERENCES [dbo].[QRTZ_JOB_DETAILS] (
                [SCHED_NAME],
                [JOB_NAME],
                [JOB_GROUP]
              );",
            ];

            using var connection = connectionFactory.CreateSqlConnection();
            await connection.OpenAsync();

            foreach (var sql in commands)
            {
                var command = connection.CreateCommand();
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync();
            }

            await connection.CloseAsync();
        }

        protected NameValueCollection GetProperties(LocalDbConnectionFactory? connectionFactory)
        {
            var result = new NameValueCollection();

            result.Add($"{StdSchedulerFactory.PropertyObjectSerializer}.type", "json");
            result.Add($"{StdSchedulerFactory.PropertySchedulerInstanceId}", StdSchedulerFactory.AutoGenerateInstanceId);
            result.Add($"{StdSchedulerFactory.PropertySchedulerInterruptJobsOnShutdownWithWait}", true.ToString());

            if (connectionFactory != null)
            {
                const string dataSourceName = "database";
                result.Add(StdSchedulerFactory.PropertyJobStoreType, typeof(JobStoreTX).AssemblyQualifiedName);
                result.Add($"{StdSchedulerFactory.PropertyJobStorePrefix}.driverDelegateType", typeof(SqlServerDelegate).AssemblyQualifiedName);
                result.Add($"{StdSchedulerFactory.PropertyJobStorePrefix}.tablePrefix", "QRTZ_");
                result.Add($"{StdSchedulerFactory.PropertyJobStorePrefix}.useProperties", true.ToString());
                result.Add($"{StdSchedulerFactory.PropertyJobStorePrefix}.dataSource", dataSourceName);
                result.Add($"{StdSchedulerFactory.PropertyJobStorePrefix}.clustered", true.ToString());
                result.Add($"{StdSchedulerFactory.PropertyDataSourcePrefix}.{dataSourceName}.{StdSchedulerFactory.PropertyDataSourceConnectionString}", connectionFactory.ConnectionString);
                result.Add($"{StdSchedulerFactory.PropertyDataSourcePrefix}.{dataSourceName}.{StdSchedulerFactory.PropertyDataSourceProvider}", "SqlServer");
            }

            return result;
        }
    }
}
