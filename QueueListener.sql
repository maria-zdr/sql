-- =============================================
-- Author:		MS
-- Create date: 2016-09-02
-- Description:	SQL Queue for importing data from devices		
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
