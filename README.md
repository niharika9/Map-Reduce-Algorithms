# Map-Reduce-Algorithms
The goal of these tasks is to perform data analysis using Spark in scala

### Environment Configuration:

IDE   : IntelliJIDE
Spark : 2.3.1 version
Scala : 2.11.0 version


### Data

“Stack Overflow 2018 Developer Survey” data . Can be found in the survey_results_public.csv.zip folder.


### Running the code: 

type the following commands in the terminal : 

````
spark-submit --class Task1 CompressedCode.jar <Path_to_inputfile : survey_results_public.csv> <outputfile:Task1.csv>
spark-submit --class Task2 CompressedCode.jar < Path_to_inputfile : survey_results_public.csv>  <outputfile:Task2.csv>
spark-submit --class Task3 CompressedCode.jar < Path_to_inputfile : survey_results_public.csv> <outputfile:Task3.csv>

````

