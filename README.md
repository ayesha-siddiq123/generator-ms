#_____________________ config __________________#

input = add your input folder path where data is there for all the states
output = add output path where you want to save data

#_______________ how to run code ______________#
cd Adapter
chmode 777 VSK_data_transformation.sh
bash VSK_data_transformation.sh      ------------ TO run the code in the same direcory where ".sh" file is there
sudo bash ./VSK_data_transformation.sh ---------- To run it from anywhere out of the directory

#_________ VSK_data_transformation.sh ___________#

python <file_name.py> <state_name> <program_name> <input_filename>
file_name.py ----> python file which you want to run
state_name   ----> for which state you want to do data transformation [which will also create output folder with sate_name inside <OUTPUT>/ folder]
program_name ----> for which program you want to do data transformation [which will also create folder with program name inside <OUTPUT>/<state>/ folder]
input_file   ----> input file of that program

