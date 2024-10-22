--For the Transaction Path table:
--Make sure field naming convention matches the other tables
--i.e. instead of Account_From it should be Account From
--Filter out the cancelled transactions
--Split the flow into incoming and outgoing transactions 
--Bring the data together with the Balance as of 31st Jan 
--Work out the order that transactions occur for each account
--Hint: where multiple transactions happen on the same day, assume the highest value transactions happen first
--Use a running sum to calculate the Balance for each account on each day (hint)
--The Transaction Value should be null for 31st Jan, as this is the starting balance

WITH CTE AS
(
SELECT D.TRANSACTION_DATE
      ,P.ACCOUNT_TO AS ACCOUNT_ID    --Receiving Account
      ,D.VALUE
      ,I.BALANCE
FROM
TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_DETAIL D 
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_PATH P ON P.transaction_id=D.transaction_id -- this join is to bring in the Receiving account from path
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION I ON ACCOUNT_ID=I.ACCOUNT_NUMBER
WHERE I.BALANCE_DATE ='2023-01-31' AND CANCELLED_='N' 

UNION ALL

SELECT D.TRANSACTION_DATE
      ,P.ACCOUNT_FROM AS ACCOUNT_ID  --Payee Account
      ,D.VALUE*-1  -- Needs to be neagive since money is going out
      ,I.BALANCE
FROM
TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_DETAIL D 
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_PATH P ON P.transaction_id=D.transaction_id --this join is to bring in the Receiving account from path
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION I ON ACCOUNT_ID=I.ACCOUNT_NUMBER
WHERE I.BALANCE_DATE ='2023-01-31' AND CANCELLED_='N'

UNION ALL

SELECT BALANCE_DATE AS TRANSACTION_DATE
      ,ACCOUNT_NUMBER AS ACCOUNT_ID
      ,NULL AS VALUE
      ,BALANCE
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION
)
SELECT *
    ,SUM(IFNULL(VALUE,0))OVER(PARTITION BY ACCOUNT_ID ORDER BY TRANSACTION_DATE, VALUE DESC) + BALANCE AS RS_BAL  --value descending caters for the assumption biggest transaction happening first when there are multiple transactions on a day. 
FROM CTE
ORDER BY ACCOUNT_ID, TRANSACTION_DATE, VALUE DESC; 
