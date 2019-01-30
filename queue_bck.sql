CREATE MESSAGE TYPE [RailPOS2Message] VALIDATION = WELL_FORMED_XML
GO
CREATE CONTRACT [RailPOS2Contract] ([RailPOS2Message] SENT BY ANY)
GO
CREATE QUEUE [dbo].[RailPOS2SendingQueue] WITH STATUS = ON , RETENTION = OFF , POISON_MESSAGE_HANDLING (STATUS = ON)  ON [PRIMARY] 
GO
CREATE QUEUE [dbo].[RailPOS2ReceivingQueue] WITH STATUS = ON , RETENTION = OFF , ACTIVATION (  STATUS = ON , PROCEDURE_NAME = [dbo].[QueueListener] , MAX_QUEUE_READERS = 1 , EXECUTE AS OWNER  ), POISON_MESSAGE_HANDLING (STATUS = ON)  ON [PRIMARY] 
GO
CREATE SERVICE [RailPOS2SendingService]  ON QUEUE [dbo].[RailPOS2SendingQueue] ([RailPOS2Contract])
GO
CREATE SERVICE [RailPOS2ReceivingService]  ON QUEUE [dbo].[RailPOS2ReceivingQueue] ([RailPOS2Contract])
GO


-- =============================================
-- Description:	SQL Queues test proc

-- Errors: Service Broker received an error message on this conversation. Service Broker will not transmit the message; it will be held until the application ends the conversation.
-- The server principal "XXXX" is not able to access the database "RailPOS2" under the current security context.
-- =============================================

ALTER PROCEDURE [dbo].[QueueListener]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE	@dialogHandle UNIQUEIDENTIFIER,
			@messageTypeName SYSNAME,
			@trancount INT,
			@messageBody VARCHAR(MAX),
			@errorNumber INT,
			@errorMessage NVARCHAR(4000),
			@IsMessageAvailable BIT,
			@xactState INT;

	SET @IsMessageAvailable = 0
	SET @trancount = @@TRANCOUNT

	IF @trancount = 0
		BEGIN TRANSACTION
	ELSE
		SAVE TRANSACTION QueueListener;

	BEGIN TRY;
		RECEIVE TOP(1) @dialogHandle = conversation_handle, @messageTypeName = message_type_name, @messageBody = message_body FROM [RailPOS2ReceivingQueue];

		IF @messageTypeName = N'RailPOS2Message'
		BEGIN
			-- Set the flag so that the transaction can commit and the next message can be read from the queue
			SET @IsMessageAvailable = 1
		END
		ELSE IF @messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
		BEGIN
			-- Read the LBO that we were sent in the message body.
			DECLARE @nCode INT;
			DECLARE @vchDescription NVARCHAR(2000);
			DECLARE @xmlError XML;

			SELECT  @xmlError = CONVERT(XML, @messageBody);
			WITH XMLNAMESPACES( DEFAULT 'http://schemas.microsoft.com/SQL/ServiceBroker/Error')

			SELECT
				@nCode = Errors.error.value( 'Code[1]', 'int'),
				@vchDescription = Errors.error.value( 'Description[1]', 'nvarchar(max)')
			FROM @xmlError.nodes( '/Error[1]') AS Errors(error);

			RAISERROR('Service broker error received. Error %d:%s. (%s)', 0, 0, @nCode, @vchDescription, '%PROC%') WITH nowait;
		END
		ELSE IF @messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
			END CONVERSATION @dialogHandle;
	 
		IF @trancount = 0
		COMMIT;
		
		print '@IsMessageAvailable = ' + cast (@IsMessageAvailable as varchar)

		IF @IsMessageAvailable = 1
		begin
			DECLARE @xml XML;
			SELECT  @xml = CONVERT(XML, @messageBody);

			DECLARE @DeviceID int = (SELECT top 1 NodeValue = C.value('(.)[1]', 'varchar(50)') FROM @xml.nodes('/Data/Params/*') AS T(C))

			DECLARE @ProcToExec sysname = 'spSave' + (	SELECT top 1 NodeName = C.value('local-name(.)', 'varchar(50)')
														FROM @xml.nodes('/Data/*') AS T(C)
														WHERE C.value('local-name(.)', 'varchar(50)') <> 'Params')
		
			EXEC @ProcToExec @XMLData = @messageBody, @DeviceID = @DeviceID
		end
	END TRY
	BEGIN CATCH
		SET @errorNumber = ERROR_NUMBER();
		SET @errorMessage = ERROR_MESSAGE();
		SET @xactState = XACT_STATE();

		IF @xactState = -1 OR @xactState = 1
		BEGIN
			ROLLBACK;
		END

		DECLARE @Module nvarchar(100)=OBJECT_NAME(@@PROCID)
		EXEC dbo.[spLogMessage] @Module, @errorMessage
		 
		-- End the conversation
		IF EXISTS ( SELECT  1
					FROM    sys.conversation_endpoints
					WHERE   conversation_handle = @dialogHandle )
			END CONVERSATION @dialogHandle
			WITH ERROR = @errorNumber DESCRIPTION = @errorMessage;
	END CATCH
END
