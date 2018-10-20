REM File: kaggle_schema.sql
REM Date Created: 2018-10-12
REM Author(s): Mahkah Wu
REM Purpose: Uses csvkit to generate sql schema based on csv file contents

csvsql -i postgresql .\AllstarFull.csv
csvsql -i postgresql .\Appearances.csv
csvsql -i postgresql .\AwardsManagers.csv
csvsql -i postgresql .\AwardsPlayers.csv
csvsql -i postgresql .\AwardsShareManagers.csv
csvsql -i postgresql .\AwardsSharePlayers.csv
csvsql -i postgresql .\Batting.csv
csvsql -i postgresql .\BattingPost.csv
csvsql -i postgresql .\CollegePlaying.csv
csvsql -i postgresql .\Fielding.csv
csvsql -i postgresql .\FieldingOF.csv
csvsql -i postgresql .\FieldingOFsplit.csv
csvsql -i postgresql .\FieldingPost.csv
csvsql -i postgresql .\HallOfFame.csv
csvsql -i postgresql .\HomeGames.csv
csvsql -i postgresql .\Managers.csv
csvsql -i postgresql .\ManagersHalf.csv
csvsql -i postgresql .\Master.csv
csvsql -i postgresql .\Parks.csv
csvsql -i postgresql .\Pitching.csv
csvsql -i postgresql .\PitchingPost.csv
csvsql -i postgresql .\Salaries.csv
csvsql -i postgresql .\Schools.csv
csvsql -i postgresql .\SeriesPost.csv
csvsql -i postgresql .\Teams.csv
csvsql -i postgresql .\TeamsFranchises.csv
csvsql -i postgresql .\TeamsHalf.csv
