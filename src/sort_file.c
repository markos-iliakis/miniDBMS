#include "sort_file.h"
#include <unistd.h>

SR_ErrorCode SR_Init() {

  return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {
    printf("I am CreateFile\n");

    int fileDesc;
    BF_Block *block;
    BF_Block_Init(&block);

    // create the file and allocate one block with metadata
    EXEC_OR_DIE(BF_CreateFile(fileName));
    EXEC_OR_DIE(BF_OpenFile(fileName, &fileDesc));
    EXEC_OR_DIE(BF_AllocateBlock(fileDesc, block));

    // copy the data and unpin the block
    char *code = "sr_file", *data;
    data = BF_Block_GetData(block);
    memcpy(data, code, BF_BLOCK_SIZE);
    BF_Block_SetDirty(block);
    EXEC_OR_DIE(BF_UnpinBlock(block));

    BF_CloseFile(fileDesc);
    printf("CreateFile OK\n");
    return SR_OK;
}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
    printf("I am OpenFile\n");

    BF_Block *block;
    BF_Block_Init(&block);
    char* data;

    // open the file and gain the metadata to note if we are in sr file
    EXEC_OR_DIE(BF_OpenFile(fileName, fileDesc));
    EXEC_OR_DIE(BF_GetBlock(*fileDesc, 0, block));
    data = BF_Block_GetData(block);
    EXEC_OR_DIE(BF_UnpinBlock(block));
    if(strcmp(data, "sr_file") != 0)
        printf("This is not a sort file\n");

    printf("OpenFile OK\n");
  return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
  printf("I am CloseFile\n");

  EXEC_OR_DIE(BF_CloseFile(fileDesc));

  printf("CloseFile OK\n");
  return SR_OK;
}

SR_ErrorCode SR_InsertEntry(int fileDesc, Record record) {
  BF_Block* block;
  BF_Block_Init(&block);
  BF_Block* block2;
  BF_Block_Init(&block2);
  char* data;
  int blocks_num;
  int counter;

  EXEC_OR_DIE(BF_GetBlockCounter(fileDesc, &blocks_num));

  if(blocks_num == 1){                                        //If in the file we have only the metadata block
    EXEC_OR_DIE(BF_AllocateBlock(fileDesc, block));           //Allocate a block based on the descriptor and return address
    data = BF_Block_GetData(block);                           //Get the data from the block
    memcpy(data, "0", 4);                                     //set a counter of the entries in the start of the block
    blocks_num++;
    BF_UnpinBlock(block);
  }

  EXEC_OR_DIE(BF_GetBlock(fileDesc, blocks_num-1, block));    //Get the specific Block from disk and Pin it
  data = BF_Block_GetData(block);
  counter = atoi(data);
  if(counter < BF_BLOCK_SIZE / sizeof(record)){               //if there is empty space for records

  	memcpy(data+4+counter*sizeof(Record), &record, sizeof(Record));     //store the record to the block

    counter++;                                                //raise the counter
    sprintf(data, "%d", counter);							  //copy counter back to block
  }
  else{

  	EXEC_OR_DIE(BF_UnpinBlock(block));                        //Unpin the block as we stopped using it
    EXEC_OR_DIE(BF_AllocateBlock(fileDesc, block));           //Allocate a block based on the descriptor and return address
    data = BF_Block_GetData(block);                           //Get the data from the block
    memcpy(data, "1", sizeof(int));
    memcpy(data+4, &record, sizeof(record));                  //store the record to the block
  }

  BF_Block_SetDirty(block);                                   //Set the block Dirty to write it back to disk
  EXEC_OR_DIE(BF_UnpinBlock(block));                          //Unpin the block as we stopped using it
  return SR_OK;
}

SR_ErrorCode SR_SortedFile(const char* input_filename, const char* output_filename, int fieldNo, int bufferSize){
    printf("I am SortedFile\n");

    int fileDesc, fileDesc2, fileDesc3, blocks_num1, blocks_num2, blocks_num3, last_block;
    BF_Block *buffer[bufferSize];
    for (int j = 0; j < bufferSize; j++)
        BF_Block_Init(&buffer[j]);

    char *data[2];
    int input_blocks;
    BF_Block *old_block, *new_block;
    BF_Block_Init(&old_block);
    BF_Block_Init(&new_block);

    // open input file
    EXEC_OR_DIE(BF_OpenFile(input_filename, &fileDesc));
    //SR_PrintAllEntries(fileDesc);

    EXEC_OR_DIE(BF_GetBlockCounter(fileDesc, &blocks_num1));

    // initialize output file
    SR_CreateFile(output_filename);
    SR_OpenFile(output_filename, &fileDesc2);

    EXEC_OR_DIE(BF_GetBlockCounter(fileDesc2, &blocks_num2));

    // initialize temp file
    SR_CreateFile("temp_output");
    SR_OpenFile("temp_output", &fileDesc3);

    //for each n-blocks in input file (n = bufferSize)
    for (int i = 1;  i < blocks_num1; i+=bufferSize) {

        // add each one to buffer
        for (int j = 0; j < bufferSize; j++) {
            if(i+j == blocks_num1){
                last_block = j-1;
                break;
            }
            EXEC_OR_DIE(BF_GetBlock(fileDesc, i+j, buffer[j]));
            last_block = j;

        }

        //quicksort them
        sort(buffer, fieldNo, last_block);

        // write them in temp_output
        //for every block in buffer
        for (int k = 0; k < last_block + 1; k++) {
            EXEC_OR_DIE(BF_AllocateBlock(fileDesc3, new_block));
            data[0] = BF_Block_GetData(buffer[k]);
            data[1] = BF_Block_GetData(new_block);
            memcpy(data[1], data[0], BF_BLOCK_SIZE);
            BF_UnpinBlock(buffer[k]);
            BF_Block_SetDirty(new_block);
            BF_UnpinBlock(new_block);
        }
        // for (int k = 0; k < last_block + 1; k++) {
        //
        //     int entriesNo = getEntriesNo(buffer[k]);
        //     char* data = BF_Block_GetData(buffer[k]);
        //     int offset = 4;
        //
        //     // for every entry in block
        //     for (int l = 0; l < entriesNo; l++) {
        //         Record record;
        //         memcpy(&record, data+offset, sizeof(Record));
        //         offset += sizeof(Record);
        //
        //         // insert the entry from buffer to temp file
        //         SR_InsertEntry(fileDesc3, record);
        //     }
        //     BF_UnpinBlock(buffer[k]);
        // }
    }


    EXEC_OR_DIE(BF_GetBlockCounter(fileDesc3, &blocks_num3));

    // external sort with (phase 2)
    int runs = (blocks_num3+bufferSize-1)/bufferSize;
    int total_blocks = blocks_num3;
    int j = 0;
    int runSize = bufferSize;   //run size must be equal to the size of the existing groups
    int index = 1;
    int groups[bufferSize-1];
    int blocks_to_make;
    int last_group;
    // int y1;

    // if(bufferSize == 10-1)
    //     y = 1;
    // else
    //     y = 1;

    // while the number of runs at end of previus pass is > 1
    while(runs>1){

        // while there are runs to be merged from previus pass
        while(index < blocks_num3 - 1){

            // initialize the groups
            for (size_t i = 0; i < bufferSize-1; i++) {
                groups[i] = 0;
            }

            // read each run into an input buffer
            j = 0;
            for (int i=index; i < index+(bufferSize-1)*runSize; i+=runSize) {
                if(i+j < blocks_num3){

                    EXEC_OR_DIE(BF_GetBlock(fileDesc3, i, buffer[j]));
                    groups[j] = i;
                    j++;
                }
            }

            if(index+(bufferSize-1)*runSize <= blocks_num3)
                blocks_to_make = (bufferSize-1)*runSize;
            else{
                if(groups[j - 1] != 0)
                    blocks_to_make = blocks_num3 - groups[j-1] + (j-1)*runSize + 1;
                else
                    blocks_to_make = blocks_num3 - groups[j-1];
            }

            // merge the runs and write to the output buffer
            k_way_merge(buffer, bufferSize, runSize, fieldNo, groups, fileDesc3, blocks_to_make);

            // choose next B-1 runs
            index += (bufferSize-1)*runSize;
        }

        // find the ceil of runs/(bufferSize-1)
        runs = (runs + (bufferSize-1) -1) / (bufferSize-1);

        // move index to the start of the last pass
        index = blocks_num3;

        // get the new number of blocks after the last pass
        EXEC_OR_DIE(BF_GetBlockCounter(fileDesc3, &blocks_num3));

        runSize *= (bufferSize-1);


    }


    EXEC_OR_DIE(BF_GetBlockCounter(fileDesc, &input_blocks));
    for (int i = blocks_num3 - input_blocks+1 ; i < blocks_num3; i++) {
       BF_GetBlock(fileDesc3, i, old_block);
       EXEC_OR_DIE(BF_AllocateBlock(fileDesc2, new_block));
       data[0] = BF_Block_GetData(old_block);
       data[1] = BF_Block_GetData(new_block);
       memcpy(data[1], data[0], BF_BLOCK_SIZE);
       BF_UnpinBlock(old_block);
       BF_Block_SetDirty(new_block);
       BF_UnpinBlock(new_block);
    }

    EXEC_OR_DIE(BF_CloseFile(fileDesc3));
    EXEC_OR_DIE(BF_CloseFile(fileDesc2));

    if (remove("temp_output") != 0)
        return -6;

  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  printf("Printing Entries...\n\n");
  printf("   id  |       Name      |     Surname     |      City\n");
  printf("------------------------------------------------------------\n");

  int blocks_num, number_of_records, offset;
  Record record;
  BF_Block* block;
  BF_Block_Init(&block);
  char* data;

  EXEC_OR_DIE(BF_GetBlockCounter(fileDesc, &blocks_num));

  for(int i=1; i<blocks_num; ++i){

      EXEC_OR_DIE(BF_GetBlock(fileDesc, i, block));

      char counter[4], *data = BF_Block_GetData(block);

      printf("\n\nBlock: %d\n", i);
      printf("-------------------\n");

      for (int j = 4; j < BF_BLOCK_SIZE; j+=sizeof(record)) {

          memcpy(&record, &data[j], sizeof(record));

          printf("%5d |", record.id);
          printf("%15s  |", record.name);
          printf("%15s  |", record.surname);
          printf("%15s\n", record.city);

      }

      EXEC_OR_DIE(BF_UnpinBlock(block));
  }

  return SR_OK;
}

void sort(BF_Block** buffer, int fieldNo, int bufferSize){

    // take the entries counter of the last block
    int entriesNo = getEntriesNo(buffer[bufferSize]);
    quicksort(buffer, fieldNo, 4, 0, sizeof(int)+(entriesNo-1)*sizeof(Record), bufferSize, bufferSize-1);
}

//high thesi teleuataias eggrafhs sto teleutaio block
//high_block arithmos block sto opoio brisketai h eggrafh pou deixnei h high
//low thesi teleutaias eggrafhs sto prwto block
//low_block arithmos block sto opoio brisketai h eggrafh pou deixnei h low
void quicksort(BF_Block** buffer, int fieldNo, int low, int low_block, int high, int high_block, int bufferSize){
    part_return partition_value;
    int new_high, new_low, new_high_block, new_low_block;


    if(high >= 0 && low >= 0 && low_block >= 0 && high_block >=0){


        if(low_block < high_block || (low_block == high_block && low < high)){
            partition_value = partition(buffer, fieldNo, low, low_block, high, high_block, bufferSize);

            //an h timh pou epestrepse h partition deixnei sthn prwth eggrafh block
            //tote pare thn teleutaia eggrafh tou prohgoumenou block gia high
            //alliws

            if((partition_value.pos - (int)sizeof(Record)) < 0){

                new_high = BF_BLOCK_SIZE - sizeof(Record);
                new_high_block = partition_value.block - 1;
            }
            else{
                new_high = partition_value.pos - sizeof(Record);
                new_high_block =  partition_value.block;
            }
            if(partition_value.pos + sizeof(Record) == BF_BLOCK_SIZE){
                new_low = 4;
                new_low_block = partition_value.block + 1;
            }
            else{
                new_low = partition_value.pos + sizeof(Record);
                new_low_block = partition_value.block;
            }
            quicksort(buffer, fieldNo, low, low_block, new_high, new_high_block, bufferSize);
            quicksort(buffer, fieldNo, new_low, new_low_block, high, high_block, bufferSize);
        }
    }
}

part_return partition(BF_Block** buffer, int fieldNo, int low, int low_block, int high, int high_block, int bufferSize){

    part_return returning_values;
    Record pivot_record, j_record, i_record;
    char *data1 = BF_Block_GetData(buffer[high_block]);    //for the block of pivot
    char *data2 = BF_Block_GetData(buffer[low_block]);     //for the block of i
    char *data3 = BF_Block_GetData(buffer[low_block]);     //for the block of j
    int i_block = low_block;
    memcpy(&pivot_record, data1+high, sizeof(Record));
    int i = low - sizeof(Record);
    int h = BF_BLOCK_SIZE;
    while ( low_block <= high_block) {

        if(low_block == high_block)
            h = high;

        for (int j = low; j < h; j+=sizeof(Record)) {
            memcpy(&j_record, data3+j, sizeof(Record));
            if(compare(&pivot_record, &j_record, fieldNo) > 0){
                i += sizeof(Record);
                if(i >= BF_BLOCK_SIZE){
                    i = 4;  // show to the first entry of next block
                    i_block++;
                    data2 = BF_Block_GetData(buffer[i_block]);
                }
                memcpy(&i_record, data2+i, sizeof(Record));
                swap(&i_record, data2, i, &j_record, data3, j);
            }
        }
        low_block++;
        low = 4;
        if(low_block<=high_block)
            data3 = BF_Block_GetData(buffer[low_block]);     //for the block of j
    }
    if(i + sizeof(Record) == BF_BLOCK_SIZE){
        i = 4;
        i_block++;
        data2 = BF_Block_GetData(buffer[i_block]);
    }
    else{
        i += sizeof(Record);
    }
    returning_values.pos = i;
    returning_values.block = i_block;

    memcpy(&i_record, data2+i, sizeof(Record));
    if(compare(&i_record, &pivot_record, fieldNo) > 0)
        swap(&i_record, data2, i, &pivot_record, data1, high);

    return returning_values;
}

//Swap funcion for swapping records an storing recrds in correct blocks
void swap(Record *record1, char *data1, int offset1, Record *record2, char *data2, int offset2){

    Record temp;
    memcpy(&temp, data1+offset1, sizeof(Record));
    memcpy(&temp, data2+offset2, sizeof(Record));


    memcpy(&temp, record1, sizeof(Record));
    memcpy(record1, record2, sizeof(Record));
    memcpy(record2, &temp, sizeof(Record));

    //store in blocks
    memcpy(data1+offset1, record1, sizeof(Record));
    memcpy(data2+offset2, record2, sizeof(Record));
}

//compare function for comparing values in records
int compare(Record *pivot, Record *record, int type){
    switch (type) {
        case 0:
            if(pivot->id > record->id)
                return 1;
            else if(pivot->id == record->id)
                return 0;
            else
                return -1;
            break;
        case 1:
            return strcmp(pivot->name, record->name);
            break;
        case 2:
            return strcmp(pivot->surname, record->surname);
            break;
        case 3:
            return strcmp(pivot->city, record->city);
            break;
        default:
            return 0;
    }
}

int getEntriesNo(BF_Block *block){
    char* data = BF_Block_GetData(block);
    char a[4];
    memcpy(a, data, sizeof(int));
    return atoi(a);
}

int k_way_merge(BF_Block** buffer, int buffersize, int runSize, int fieldNo, int *groups, int filedesc, int new_blocks){

    EXEC_OR_DIE(BF_AllocateBlock(filedesc, buffer[buffersize-1]));

    Record records[buffersize - 1];

    Record temp;

    int min_position;
    int range;
    int check = 1, check2 = 1;
    int offset[buffersize];
    int group_offset[buffersize-1];
    char *data[buffersize];
    int count = 0;

    // get all 1st blocks of bufferSize-1 runs into the buffer
    for (int i = 0; i < buffersize; i++) {
        if(groups[i] == 0){
            data[i] = NULL;
            offset[i] = BF_BLOCK_SIZE;
        }
        else{
            data[i] = BF_Block_GetData(buffer[i]);
            offset[i] = 4;
        }
    }

    // initialize the block offset
    for (int i = 0; i < buffersize-1; i++) {
        if(groups[i] == 0)
            group_offset[i] = -1;
        else
            group_offset[i] = groups[i];
    }
    // pointers of blocks < runSize and not all entries scanned

    if(new_blocks == 2*runSize)
        range = runSize;
    else if(new_blocks < runSize)
        range = new_blocks + 1;
    else
        range = runSize;

    while(check && check2){


        int flag = 1;
        for (int i = 0; i < buffersize-1; i++) {

            // if we have not reached the end of the last block of this buffer
            if(data[i] != NULL){

                // if we process the first record of the first buffer set it as temp
                if(flag){
                    memcpy(&temp, data[i] + offset[i], sizeof(Record));
                    flag = 0;
                }

                // make the record to compare
                memcpy(&records[i], data[i] + offset[i], sizeof(Record));

                // compare all bufferSize-1 elements for the smallest
                if(compare(&temp, &records[i], fieldNo) >= 0 ){

                    memcpy(&temp, &records[i], sizeof(Record));

                    min_position = i;
                }
            }
        }

        // write the smallest to the output block of buffer
        if(temp.id == 0 && strcmp(temp.name, "") == 0 && strcmp(temp.surname, "") == 0 && strcmp(temp.city, "") == 0){
            memcpy(&temp, data[min_position] + 4, sizeof(Record));
            sprintf(data[buffersize - 1], "%d", 14);
            BF_Block_SetDirty(buffer[buffersize-1]);
            BF_UnpinBlock(buffer[buffersize-1]);
            data[min_position] = NULL;
            offset[min_position] = BF_BLOCK_SIZE;
            group_offset[min_position] =  groups[min_position] + range;
            new_blocks--;
        }
        else{

            Record temp3;
            memcpy(&temp3, data[buffersize-1] + offset[buffersize - 1] - sizeof(Record) ,sizeof(Record));
            if(temp.id == temp3.id && strcmp(temp.name, temp3.name) == 0 && strcmp(temp.surname, temp3.surname) == 0 && strcmp(temp.city, temp3.city) == 0){
                sprintf(data[buffersize - 1], "%d", 14);
                BF_Block_SetDirty(buffer[buffersize-1]);
                BF_UnpinBlock(buffer[buffersize-1]);
                break;
            }
            else{
                memcpy(data[buffersize-1] + offset[buffersize - 1], &temp, sizeof(Record));

                // move record's block index down
                offset[buffersize-1] += sizeof(Record);
                offset[min_position] += sizeof(Record);
            }

        }


        //if the block has run out , move to the next of its list
        if(offset[min_position] == BF_BLOCK_SIZE && groups[min_position] != 0){

            BF_UnpinBlock(buffer[min_position]);

            if(group_offset[min_position] < groups[min_position] + range - 1 ){
                group_offset[min_position]++;
                EXEC_OR_DIE(BF_GetBlock(filedesc, group_offset[min_position], buffer[min_position]));
                data[min_position] = BF_Block_GetData(buffer[min_position]);
                offset[min_position] = 4;
            }
            else{
               data[min_position] = NULL;
               offset[min_position] = -1;
            }
        }

        //if the buffer output block has run out, write it to the disk and empty it
        if(offset[buffersize-1] == BF_BLOCK_SIZE && new_blocks > 0){
            new_blocks--;
            sprintf(data[buffersize - 1], "%d", 17);
            BF_Block_SetDirty(buffer[buffersize-1]);
            BF_UnpinBlock(buffer[buffersize-1]);

            if(new_blocks > 0 ){
                offset[buffersize-1] = 4;
                EXEC_OR_DIE(BF_AllocateBlock(filedesc, buffer[buffersize-1]));
                data[buffersize-1] = BF_Block_GetData(buffer[buffersize-1]);
            }
        }

        //check if there are still block and entries in these blocks for merging
        check = 0;
        check2 = 0;
        for (int k = 0; k < buffersize-1; k++) {
            if(offset[k] > 0){
                check = 1;
            }
            if(group_offset[k] < groups[min_position] + range && groups[k] != 0){
                check2 = 1;
            }
            if(new_blocks == 0){
                check = 0;
                check2 = 0;
            }
        }
    }

    return 0;

}
