# What software is required to run the data script:
- Prior to the usage of this program, the following steps must be done...
## For Anaconda:
1. Install pandas: 	`conda install pandas`
2. Install pyarrow:	`conda install -c conda-forge pyarrow`
# How to run the data script:
- There are a number of ways to run the data script. 
## By running wpAndPc.py in the command line in Windows:
1. Open the command prompt
	- Navigate to the start menu and type `cmd`.
2. In the command prompt, navigate to the directory that contains the `wpAndPc.py`.
3. Type in `python wpAndPc.py` and press enter. The program will then run.
## By running wpAndPc.py in the Anaconda Powershell Prompt:
1. Open the Anaconda Powershell Prompt
2. In the Powershell, navigative to the directory that contains the `wpAndPc.py`.
3. Type in `python wpAndPc.py` and press enter. The program will then run.
## In Jupyter Notebook
1. Open Jupyter notebook.
2. Load the `wpAndPc.ipynb` file.
3. Run the program. 
# What software is required to run the matches script:
- Prior to the running of this program, the following must be done...
## For Anaconda:
1. Install pandas: 	`conda install pandas`
2. Install pyarrow:	`conda install -c conda-forge pyarrow`
3. Install iPython:	`conda install ipython`
4. Install numpy:	`conda install -c anaconda numpy`
5. Install scipy:	`conda install -c anaconda scipy`
6. Install spacy:	`conda install -c conda-forge spacy`
7: Install the spacy model:	`python -m spacy download en_core_web_lg`
## Running the Matches script:
1. Open Jupyter notebook.
2. Load the `matches.ipynb` file.
3. Run the program. 
## How to get Matches using the script:
- The code will keep running until the "Would you like to compare another code? (1 for Yes, 0 for No)" option appears after at least one match has been made. If this option appears and the user types in 0 for No, then the program will end. 
- Once the program has begun, the first option will appear. This first option asks if the code that you would like to compare is a BLS or NAPCS code. 
	-  For example, if the code that you have is a BLS code, type in "BLS", the code, and the number of matches you'd like, 
	and the program will return the nearest N NAPCS matches. 
- Type in the code that you would like to compare.
- Type in the number of nearest matches that you would like to see, the program will return the N nearest matches to the code that you put in. 
- Once all of this has been entered the program will produce a table containing the following.
	- The code, the description, and the similarity of each of the matches. 
- After the table appears, the option to compare another code will appear.
	- You can either choose to repeat the process again by typing in 1, or 0 to end the program.
