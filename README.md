# BigData

****README****

3 MapReduce programs created for the following:
************************************************

Project Name: WordCount
Class Name: LetterCount

Counts and displays the count of words with same starting letter after handling special characters and converting to lower case.


*****************************************************************************************

Project Name: TableJoin
Class Name: TableJoin

Performs join of 2 tables. Also implemented to add the timestamp and recognize header and display on top of the table.  For this overridden the cleanup method in Reducer.

Separate mappers for each Table created. 3 arguments to be sent while invoking the hadoop execution script.  Order of tables is 1) user 2) page_view 3) Output location

Join performed on column user_id

Hadoop execution script :
hadoop jar /Users/Vijitha/Desktop/TableJoin.jar TableJoin /user/hadoop/tablejoin/input/user.txt /user/hadoop/tablejoin/input/page_view.txt /user/hadoop/tablejoin/output


*****************************************************************************************

Project Name: ThreeTableJoin
Class Name: ThreeTableJoin

Performs join of 3 tables.  <Header not displayed at top but can be done similarly to previous program using the Cleanup method in Reducer>

Separate mappers for each Table created. 3 arguments to be sent while invoking the hadoop execution script.  Order of tables is 1) user_age 2) page_view 3) user_gender 4)Output location

Join performed on column user_id

Hadoop execution script:
hadoop jar /Users/Vijitha/Desktop/ThreeTableJoin.jar ThreeTableJoin /user/hadoop/threetablejoin/input/user_age.txt /user/hadoop/threetablejoin/input/page_view.txt /user/hadoop/threetablejoin/input/user_gender.txt /user/hadoop/threetablejoin/output

