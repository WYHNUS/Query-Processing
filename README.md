# CS3223:   Database Systems Implementation

### 1. Project Description

### 2. Environment Setup

1. Create a folder named _classes_ under the root folder which serves as the output directory for the project.

2. In project root folder, create a system variable COMPONENT pointing to the root directory:

    Linux / Mac : execute command 
 ``
 source queryenv
 ``
 
    Windows : follow the [guide](http://www.comp.nus.edu.sg/~tankl/cs3223/project/cs3223-proj-setup.htm)

    Eclipse (other IDEs such as IntelliJ are similar) : follow the [guide](http://www.comp.nus.edu.sg/~tankl/cs3223/project/cs3223-proj-setup.htm)

 
### 3. Compilation

Windows : execute _build.bat_ file

Linux / Mac : execute _build.sh_ file via 
``
./build.sh
``

### 4. Testing Procedure

#### 4.1 Build test cases:

Sample testing files are provided in _testcases_ directory.

Please refer to this [guide](http://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm) for more details.

#### 4.2 Test:

Eclipse / IntelliJ : 

- Copy the test cases generated in previous step into the root folder.
Run the project with extra parameters _query.in_ _query.out_
 
Command line :

- Copy the test cases generated in previous step into the _classes_ folder.
- Run the project
 ``
 java QueryMain _query.in_ _query.out_
 ``