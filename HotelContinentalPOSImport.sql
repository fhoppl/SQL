DECLARE @Innhold AS VARCHAR(max)

--DECLARE @HeaderValues AS Table(Value VARCHAR(max), counter INT Identity(1,1))

 DECLARE @LastImportId AS INT
 DECLARE @ImportId AS UniqueIdentifier

DECLARE @PK INT
DECLARE @Filename VARCHAR(500)
DECLARE @ThreadGuid AS UniqueIdentifier

DECLARE filecursor CURSOR FOR
SELECT TOP 1 Filnavn FROM _ImportedFileData_01 WHERE Behandlet is null AND Filnavn like 'OG%' ORDER BY PK ASC

OPEN filecursor

FETCH NEXT FROM filecursor INTO @Filename

PRINT 'Behandler fil: ' + @Filename


WHILE  @@FETCH_STATUS = 0

BEGIN


	

	DECLARE cursor_db CURSOR 

		FOR SELECT PK, Flatfilinnhold FROM [_ImportedFileData_01]  WHERE Filnavn like 'OG%' ORDER BY PK ASC

		OPEN cursor_db 

		FETCH NEXT FROM cursor_db INTO @PK, @Innhold;

		WHILE @@FETCH_STATUS = 0

		BEGIN
			
			PRINT 'Innhold: ' + @Innhold
			IF (SUBSTRING(@Innhold,1,4) = '"OH"')
	        BEGIN

				PRINT 'Behandler ordrehode'
			
				SELECT @ThreadGuid = NewId()
				UPDATE _ImportedFileData_01 SET PortalImportId = @ThreadGuid WHERE PK = @PK

				TRUNCATE TABLE HelperTable
				

				INSERT INTO HelperTable SELECT * FROM splitStringWIthQuotes(@Innhold,',')

				SELECT * FROM HelperTable


               INSERT INTO VNetCustomerInvoice(PortalImportId, importOperation, referenceNumber, documentDate, customerNumber, billingaddress_overrideAddress, billingaddress_addressLine1, billingaddress_addressLine2, billingaddress_AddressLine3, billingaddress_postalCode, billingAddress_City, billingaddress_countryID )
		
                SELECT @ThreadGuid,'Create', [2],TRY_CAST( SUBSTRING([3],5,4)+'-'+SUBSTRING([3],3,2)+'-'+SUBSTRING([3],1,2) AS datetime), [7], 1, [9],[10],[11],[12],[13],[14] 
			    FROM
                (
                      SELECT Value, counter FROM HelperTable
                 ) t
                PIVOT
				(MAX(Value) FOR counter IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21])
                )
                 AS PivotTable
							   
				
				
			END

            ELSE
			
                BEGIN

                                TRUNCATE TABLE HelperTable

                                INSERT INTO HelperTable
                                SELECT * FROM splitStringWIthQuotes(@Innhold,',')
								--, (ROW_NUMBER() OVER (ORDER BY (SELECT 1))) AS number
								SELECT @ImportId = NewID()
								SELECT @LastImportId = MAX(ImportId) FROM VNetCustomerInvoice
 
						
								
                           		INSERT INTO VNetCustomerInvoiceLines(PortalImportId, importOperation, operation, description, quantity, unitPriceInCurrency, accountNumber, vatCodeId)
		
                                SELECT @ThreadGuid,'Create', 'Insert', [3] , [16], [12], '1740','7'
								 FROM

                                (

                                                                SELECT Value, counter FROM HelperTable

                                ) t

                                PIVOT

                                (MAX(Value) FOR counter IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[101],[102],[103],[104],[105],[106],[107],[108],[109],[110],[111],[112],[113],[114],[115],[116],[117])

                                )

                                AS PivotTable
							   
							   
							   SELECT * FROM HelperTable

 
 


                END
			UPDATE _ImportedFileData_01 SET Behandlet = GETDATE() WHERE PK = @PK
 

    FETCH NEXT FROM cursor_db INTO

        @PK, @Innhold

 

END


CLOSE cursor_db
deallocate cursor_db

FETCH NEXT FROM filecursor INTO @Filename
END

CLOSE filecursor
deallocate filecursor


-- DELETE FROM VNetCustomerInvoice
-- DELETE FROM VNetCustomerInvoiceLines
-- UPDATE _ImportedFileData_01 SET Behandlet = null


