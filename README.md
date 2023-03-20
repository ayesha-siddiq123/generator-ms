#_______________ how to run code ______________#

cd adapter
chmode 777 VSK_data_transformation.sh
bash VSK_data_transformation.sh      ------------ TO run the code in the same directory where ".sh" file is there
sudo bash ./VSK_data_transformation.sh ---------- To run it from anywhere out of the directory

#_________ VSK_data_transformation.sh ___________#

python <file_name.py> <program_name> <input_filename>
file_name.py ----> python file which you want to run
program_name ----> for which program you want to do data transformation [which will also create folder with program name inside <OUTPUT>/<state>/ folder]
input_file   ----> input file of that program

