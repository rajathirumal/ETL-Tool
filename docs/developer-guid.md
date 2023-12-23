## 🏁 Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

The current implementation is focused on the [SQlite](https://sqlite.org/i) DB.
<!-- See [Running with Docker](#-running-with-docker) if you want to setup the API faster with Docker. -->


### Prerequisites
The only thing we need is [Python](https://www.python.org/) version 3.10.

Rest of the modiles are listed in the PipFile which could be installed during [setup](#setup).

### ⚙️ Setup

I personally do not like installing thirdparty for managing the environment. You are free to use any.

Create virtual environment
```sh 
python venv -m venv 
```
Activate to the venv 
```sh
source venv/bin/activate 
```
Install the required packages listed in [PipFile](/PipFile). Also attached a [requirements](/requirements) file for reference.
```sh
pip install -r PipFile
```


### Sample Data
The [project.properties](/conf/project.properties) is the soul for the whole project. The following properties are ment to be as the content of this file.


<details>
  <summary>Project - section</summary>

The following are the mandatory properties inside a section named `project` :
  
- <span style="color: red;">db.sqlite.folder</span>: Holds the name of the folder under which the sqlite db is going to be created.


! This roject expects atleast one of the the DB folder to be setup. 

! As of now the below is the only property that is created as this project is done over the SQLite DB. 

! Please follow the same property naming for DB folders `db.<dbtype>.folder`. If you want to setup an additional property for DB URL use `db.<dbtype>.url`

</details>

<details>
  <summary>Landing - section</summary>

  The following are the mandatory properties inside a section named `landing` :
  
- <span style="color: red;">landing.db.source.meta.name</span>: Holds the name of the file containing metadata for the files to be loaded.

- <span style="color: red;">landing.db.source.meta.table.name</span>: Specifies the table on which the content of the metadata files is to be copied.

- <span style="color: red;">landing.db.source.meta.file.path</span>: Specifies the folder containing the source files. If your files are in the root folder, please use a dot `.`

</details>


You are all ready to explore the code 👏🏻

> Please check out our pair repository [BriggMap](https://github.com/rajathirumal/BridgeMap) In which we create SQL stored procedures using python like programming language.

Happy contributing 🤓