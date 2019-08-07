# ClearScore Data Engineering Tech Test

This project provides the functionalities required to obtain some insights over ClearScore data in terms of:

1) Average Credit Score across a set of credit reports from ClearScore;
2) Number of users grouped by their employment status;
3) Number of users in score ranges;
4) Enriched bank data;

## Pre-Requisites

* JRE/JDK 8 (Or Higher)
* SBT 1.X
* Scala 2.12.8 (Optional)
* git


## Cloning the project

```bash
git clone https://github.com/DiogoReisPinto/clearscore-test.git
```

## Compile, Running Unit tests and Run the application

For compiling the code and running the unit tests suite using SBT:

```bash
sbt compile test
```

The entry point for this application will execute all the necessary steps to obtain the insights required based on the following inputs:

* Filesystem location for accounts input files;
* Filesystem location for credit reports input files;
* Filesystem location for the output files;

So to execute the application, we can execute the following command:

```bash
sbt "run /PATH/TO/ACCOUNT/FILES /PATH/TO/REPORT/FILES /PATH/TO/OUTPUT_FILES"
```
Where all the paths provided must exist in the local filesystem and contain the corresponding files. The files can be on subdirectories under the main directory provided.

### Execution Example

Assuming we have the following locations in our filesystem:

* Accounts Data: `/Users/Diogo/Desktop/bulk-reports/accounts`
* Reports Data: `/Users/Diogo/Desktop/bulk-reports/reports`
* Output Location: `/Users/Diogo/Desktop/bulk-reports/output`

We could run the following command to execute the application:

```bash
sbt "run /Users/Diogo/Desktop/bulk-reports/accounts /Users/Diogo/Desktop/bulk-reports/reports /Users/Diogo/Desktop/bulk-reports/output"
```

After a successful execution, we would get the output files in the Output location: `/Users/Diogo/Desktop/bulk-reports/output`. Each output refers to the exercise number, i.e, for exercise 1 we will have the output in the file `1.csv`

## Documentation available

It is possible to generate the documentation automatically for the functionalities provided using scaladoc. This documentation can be generated at any time using the command:

```bash
sbt doc
```

## Implementation considerations

* For users with `null` EmploymentStatus we are using the label "N/D"
* We assume that the credit report files are numbered under sequential identifiers and for that reason, if for a given user we have reports `1.json` and `2.json`, we assume the `2.json` report file is the latest one
