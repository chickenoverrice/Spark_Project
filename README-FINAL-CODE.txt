====OVERVIEW====
1. Folder 'input-data' contains two folders 'final-data' and 'pi_school' that are used for analysis.
2. Folder 'code' contains two textfiles of analysis codes written with Scala in Spark shell.
3. Folder 'screenshot' contains screenshots showing the programs running and files saved successfully.


====INPUT DATA====
1. The raw data were obtained from the following links:
a) NIH grant/publication data: https://exporter.nih.gov/ExPORTER_Catalog.aspx?sid=2&index=0
b) Impact factor data: http://www.scimagojr.com/journalrank.php
c) University rank: https://www.usnews.com/best-graduate-schools/top-science-schools/biological-sciences-rankings/page+10

2. Folder 'pi_school' contains the PI ID and institute for each project. Data are simply selected from the NIH grant data in ETL step. Data are used to correlate university rank with research outcome in the file 'code-analysis-per-PI'. The data schema is: {institute: string, PI_ID: string}

3. Folder 'final-data' contains relevant information about each project. Data are cleaned and transformed in ETL step. Data are used in both 'code-analysis-per-project' and 'code-analysis-per-PI' files.The data schema is: {full-project-number:string, core-project-number:string, PI-ID:string, start-time:string,study-section:string,cost:double, publiction-number:int, impact-factor:double}.

4. All input and output files are comma separated text files.


====SCREENSHOT====
1. Folder 'program-running' contains screenshots showing the running codes in Spark shell. Specifically, the screenshots show some results are successfully computed, such that we know the program is running properly.
2. Folder 'file-save-success' contains screenshots showing results successfully computed and saved in HDFS. These results are not directly reflected in the shell. Each saved result has a list of files and the content of a typical file. 


====CODE====
1. The 'code-analysis-per-project.scala' file contains program with following functionalities:
a) Performs a basic distribution analysis on all projects 
b) Analyzes the  correlation between grant size and publication number, the correlation between grant size and publication impact under three different models: i. No control variables, ii. Controlling study sections, and iii. Controllling the length of supporting year for each project (The length of supporting year is contained in the full-project-number: two digits after a '-' symbol.
c) A pearson's correlation coefficient is calculated when applicable.  
All data other than supporting year used in the codes are presented in the corresponding columns in data schema.

2. The 'code-analysis-per-PI.scala' file contains program with following functionalities:
a) Isolates the main PI for each project. The PI_IDs are concatenated as one string, the IDs are first separated and then the ID for contact PI (the leader of the project) is kept for analysis.
b) Analyzes the correlation between grant size and publication number, the correlation between grant  size and publication impact for each PI under three different models: i. No control variable, ii. Controlling the number of projects that each PI hold,s and iii. Controlling the rank of the institute that each PI is affiliated with. 
c) A pearson's correlation coefficient is calculated when applicable.
d) Tests two machine learning algorithms (linear regression and random forest with regression) in predicting the publication quantity for individual PI based on the grant size and the number of projects.

Details about programs can be found in the comments in each file.


====OUTPUT====
1. Figures in the final paper including histograms, scatter plots and Pareto plots are plotted with Excel.
2. Final results used in the data visulization are available upon request.