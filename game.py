#Working and compiling, striped partition
#Koray Tekin, Muastafa Cihan 
#   2018400213   2018400228



from mpi4py import MPI
from contextlib import redirect_stdout
import sys

comm = MPI.COMM_WORLD
total_num_of_proc = comm.Get_size()
rank = comm.Get_rank()
size_of_map  =0
number_of_waves=0
number_of_towers =0

class tower(object):						#Struct that will be placed in the matrice for each tower
	__slots__ = ["type","health"]

def singleword(input_file):					#Used for taking input word by word from input file 
	space = ''
	result = ''
	while space != ' ' and space != '\n':
		result += space
		space = input_file.read(1)
	return result

def upperCheck(type_of_node, upperRow, columnB, columnE,  currentC):   #During rounds this function is used for received upper rows
	damage=0
	if(len(upperRow)!=0):
		for tcolumn in range(columnB,columnE):
			if(upperRow[tcolumn]!=None):
				if(upperRow[tcolumn].type!=type_of_node):
					if(type_of_node==2 and (currentC==tcolumn))or(type_of_node==1):
						damage+=type_of_node
	return damage	

def lowerCheck(type_of_node, lowerRow, columnB, columnE,  currentC):	# During rounds this function is used for received lower rows
	damage=0
	if(len(lowerRow)!=0):
		for tcolumn in range(columnB,columnE):
			if lowerRow[tcolumn] != None:
				if(lowerRow[tcolumn].type!=type_of_node):
					if(type_of_node==2 and (currentC==tcolumn))or(type_of_node==1):
						damage += type_of_node
	return damage	

def middleCheck(type_of_node, twoDimensional, rowB, rowE, rowC, columnB, columnE,  currentC): #This function controls the area already existing in specific processor 
	damage=0
	for x in range(rowB, rowE):
		for y in range(columnB, columnE):
			if(x==rowC and y==currentC):
				continue
			if(twoDimensional[x][y]!=None):
				if(twoDimensional[x][y].type!=type_of_node):
					if(type_of_node==2 and (rowC==x or currentC==y))or(type_of_node==1):
						damage+=type_of_node
	return damage					

infile = open(sys.argv[1], "r")
size_of_map = int(singleword(infile))
number_of_waves = int(singleword(infile))
number_of_towers = int(singleword(infile))
if rank ==0:
	outfile = open(sys.argv[2], "w")
	numOfWave= number_of_waves
	while(numOfWave>0):
		positions_of_os = infile.readline()
		list_of_pairs_os =[]
		pos_of_os_wout_comma =""
		for i in positions_of_os:
			if(i != "," and i != "\n"):
				pos_of_os_wout_comma += i
		rowOfCoordinatesSplittedAr = pos_of_os_wout_comma.split()		
		for i in range(0,len(rowOfCoordinatesSplittedAr),2):
			list_of_pairs_os.append((int(rowOfCoordinatesSplittedAr[i]), int(rowOfCoordinatesSplittedAr[i+1])))
	
		positions_of_pluss = infile.readline()
		list_of_pairs_pluss =[]
		pos_of_pluss_wout_comma =""
		for i in positions_of_pluss:
			if(i != "," and i != "\n"):
				pos_of_pluss_wout_comma += i
		rowOfCoordinatesSplittedAr2 = pos_of_pluss_wout_comma.split()		
		for i in range(0,len(rowOfCoordinatesSplittedAr2),2):
			list_of_pairs_pluss.append((int(rowOfCoordinatesSplittedAr2[i]), int(rowOfCoordinatesSplittedAr2[i+1])))
		row_per_proc = int(size_of_map/(total_num_of_proc-1))
		for i in range(1,total_num_of_proc):
			sending_os = []
			sending_pluss = []
			begin_row_per_processor = int(row_per_proc*(i-1))
			end_row_per_processor = begin_row_per_processor + (row_per_proc-1)  
			for row,column in list_of_pairs_os:
				if(row<=end_row_per_processor and row>= begin_row_per_processor):
					sending_os.append((row,column))
			for row,column in list_of_pairs_pluss:
				if(row<=end_row_per_processor and row>= begin_row_per_processor):
					sending_pluss.append((row,column))		
			comm.send(sending_os, dest = i, tag =0)
			comm.send(sending_pluss, dest =i, tag =1)

		waveEnd = comm.recv(source=rank+1, tag=6)
		numOfWave -=1
	listOfAllResults= []	
	for resultRow in range(1,total_num_of_proc):
		partOfResult = comm.recv(source=resultRow, tag=7)
		for innerLists in partOfResult:
			listOfAllResults.append(innerLists)

	for row in range(size_of_map):
		for column in range(size_of_map-1):
			if listOfAllResults[row][column]==None:
				with redirect_stdout(outfile):
					print(". ", end="")
			elif listOfAllResults[row][column].type==2:
				with redirect_stdout(outfile):
					print("o ", end="")	
			elif listOfAllResults[row][column].type==1:
				with redirect_stdout(outfile):
					print("+ ", end="")
		if listOfAllResults[row][size_of_map-1]==None:
			if(row!=size_of_map-1):
				with redirect_stdout(outfile):
					print(".")
			else:
				with redirect_stdout(outfile):
					print(".", end="")	
		elif listOfAllResults[row][size_of_map-1].type==2:
			if(row!=size_of_map-1):
				with redirect_stdout(outfile):
					print("o")
			else:
				with redirect_stdout(outfile):
					print("o", end="")	
		elif listOfAllResults[row][size_of_map-1].type==1:
			if(row!=size_of_map-1):
				with redirect_stdout(outfile):
					print("+")
			else:
				with redirect_stdout(outfile):
					print("+", end="")		

else:
	infile.close()
	row_per_proc = int(size_of_map/(total_num_of_proc-1))
	begin_row_per_processor = int(row_per_proc*(rank-1))
	end_row_per_processor = begin_row_per_processor + (row_per_proc-1)
	numOfWaveForChilds = number_of_waves
	twoDimensional_perProcess = []
	for i in range(row_per_proc):
		tempo = [None] * size_of_map
		twoDimensional_perProcess.append(tempo)	
	while(numOfWaveForChilds > 0 ):
		listOfOsCoord = comm.recv(source=0, tag=0)
		listOfPlussCoord = comm.recv(source=0, tag =1)
		
		for row, column in listOfOsCoord:
			if(twoDimensional_perProcess[row- begin_row_per_processor][column]==None):
				node = tower()
				node.type= 2
				node.health= 6
				twoDimensional_perProcess[row- begin_row_per_processor][column] =node

		for row, column in listOfPlussCoord:
			if(twoDimensional_perProcess[row -begin_row_per_processor][column]==None):
				node = tower()
				node.type= 1
				node.health = 8
				twoDimensional_perProcess[row- begin_row_per_processor][column] = node
		
		for k in range(8):
			upperNeighbourRow=[]
			lowerNeighbourRow=[]
			deadTowersCoord = []      #pairs of row,column to be taken out after each round.
			if rank%2==1:
				if(rank != total_num_of_proc-1):
					comm.send(twoDimensional_perProcess[row_per_proc-1], dest=rank+1, tag=2)
				if(rank!=1):
					upperNeighbourRow = comm.recv(source= rank-1, tag=3)	
					comm.send(twoDimensional_perProcess[0], dest = rank-1, tag=4)
				if(rank!= total_num_of_proc-1):
					lowerNeighbourRow = comm.recv(source= rank+1, tag = 5)
			
			else:
				upperNeighbourRow = comm.recv(source= rank-1, tag=2)
				if(rank != total_num_of_proc-1):
					comm.send(twoDimensional_perProcess[row_per_proc-1], dest=rank+1, tag=3)
					lowerNeighbourRow = comm.recv(source=rank+1, tag=4)
				comm.send(twoDimensional_perProcess[0], dest=rank-1, tag=5)
			
			for row in range(row_per_proc):
				for column in range(size_of_map):
					totalDamageTaken=0
					if(twoDimensional_perProcess[row][column]!=None):
						typeOfNode = twoDimensional_perProcess[row][column].type	
						if(column!=0 and column != size_of_map-1):
							if (row !=0 and row!= row_per_proc-1):
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+2,row, column-1, column+2, column)

							elif(row==0 and row!= row_per_proc-1):
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column-1, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+2,row, column-1, column+2, column)
												
							elif(row==row_per_proc-1 and row!=0):
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column-1, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+1,row, column-1, column+2, column)

							else: 
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column-1, column+2, column)
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column-1, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+1,row, column-1, column+2, column)								

						elif(column ==0 and column != size_of_map-1):
							if (row !=0 and row!= row_per_proc-1):
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+2,row, column, column+2, column)

							elif(row==0 and row!= row_per_proc-1):
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+2,row, column, column+2, column)				
												
							elif(row==row_per_proc-1 and row!=0):
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+1,row, column, column+2, column)

							else: 
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column, column+2, column)
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column, column+2, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+1,row, column, column+2, column)

						elif(column == size_of_map-1 and column!=0):
							if (row !=0 and row!= row_per_proc-1):
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+2,row, column-1, column+1, column)
							elif(row==0 and row!= row_per_proc-1):
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column-1, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+2,row, column-1, column+1, column)
												
							elif(row==row_per_proc-1 and row!=0):
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column-1, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+1,row, column-1, column+1, column)				

							else: 
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column-1, column+1, column)
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column-1, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+1,row, column-1, column+1, column)

						else:
							if (row !=0 and row!= row_per_proc-1):
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+2,row, column, column+1, column)

							elif(row==0 and row!= row_per_proc-1):
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+2,row, column, column+1, column)

							elif(row==row_per_proc-1 and row!=0):
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row-1,row+1,row, column, column+1, column)

							else: 
								totalDamageTaken += lowerCheck(typeOfNode, lowerNeighbourRow, column, column+1, column)
								totalDamageTaken += upperCheck(typeOfNode, upperNeighbourRow, column, column+1, column)
								totalDamageTaken += middleCheck(typeOfNode, twoDimensional_perProcess,row,row+1,row, column, column+1, column)

						twoDimensional_perProcess[row][column].health -= totalDamageTaken							
												
						if(twoDimensional_perProcess[row][column].health<=0):
							deadTowersCoord.append((row,column))

			for deadRow, deadColumn in deadTowersCoord:
				twoDimensional_perProcess[deadRow][deadColumn] = None


		if(rank!=total_num_of_proc-1):
			slackVariable = comm.recv(source=rank+1,tag=6)
		message = 1
		comm.send(message, dest= rank-1, tag=6)		
		numOfWaveForChilds -=1
	comm.send(twoDimensional_perProcess, dest=0, tag=7)	
	

















