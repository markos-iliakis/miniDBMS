#include "myfunc.h"

int AM_errno;
indexes** openIndexes;
scans** openScans;

void get_metadata(char** data, BF_Block** block, int* block_num, char* block_type, int* entries_counter, int* offset){
    *offset = 0;
    char temp_char, temp_int[4];

    *data = BF_Block_GetData(*block);

    // get the block number
    memcpy(temp_int, *data, sizeof(int));
    *block_num = atoi(temp_int);

    // get the block type leaf/index
    *offset += sizeof(int);
    memcpy(block_type, *data + *offset, sizeof(char));

    // get the entry counter
    *offset += sizeof(char);
    memcpy(temp_int, *data + *offset, sizeof(int));
    *entries_counter = atoi(temp_int);

    *offset += sizeof(int);
}

void store_metadata(char **data, BF_Block** block, int block_num, char block_type, int entries_counter, int* offset){
    *offset = 0;
    *data = BF_Block_GetData(*block);
    sprintf(*data, "%d", block_num);
    *offset += sizeof(int);
    memcpy((*data) + (*offset), &block_type, 1);
    (*offset)++;
    sprintf((*data + *offset), "%d", entries_counter);
    *offset += sizeof(int);
}

int insert_node(char** data, void* value1, void* value2, int* entries_counter, int indexArray, BF_Block** block, int blocks_num, int block_num, int offset, Stack* stack){
    char block_type, temp_int[5], temp_float[4];
    int eq = 0, entry_in_block = 1;

    const int size_of_metadata = sizeof(block_num)+sizeof(block_type)+sizeof(*entries_counter);
    const int size_of_pointer = sizeof(int);
    const int size_of_entry = openIndexes[indexArray]->attrLength1 + openIndexes[indexArray]->attrLength2;

    // find a greater value inside the block
    while(1){
        if(openIndexes[indexArray]->attrType1 == 'f'){
            // copy key_value to compare
            memcpy(temp_float, (*data)+offset, openIndexes[indexArray]->attrLength1);
            float key_value = atof(temp_float);

            if(*(float *)value1 == key_value) eq = 1;

            // if we found greater key stop
            if(*(float *)value1 <= key_value)
                break;
        }
        else if(openIndexes[indexArray]->attrType1 == 'i'){
            // copy key_value to compare
            memcpy(temp_int, (*data)+offset, openIndexes[indexArray]->attrLength1);
            int key_value = atoi(temp_int);

            if( *(int *) value1 == key_value) eq = 1;

            // if we found greater key stop
            if( *(int *) value1 <= key_value)
                break;
        }
        else if(openIndexes[indexArray]->attrType1 == 'c'){
            // copy key_value to compare
            char key_value[40];
            memcpy(key_value, (*data)+offset, openIndexes[indexArray]->attrLength1);

            // if we found greater key stop
            int cmp = strcmp((char *)value1, key_value);
            if( cmp <= 0){
                if (cmp == 0) eq = 1;
                break;
            }
        }

        if (entry_in_block>(*entries_counter)) break;
        entry_in_block++;
        offset+=size_of_entry;
    }

    // if the block has space for another entry
    if((*entries_counter) < ((BF_BLOCK_SIZE-size_of_metadata-size_of_pointer)/size_of_entry)){

        char temp[BF_BLOCK_SIZE];
        memcpy(temp, (*data), BF_BLOCK_SIZE);

        // empty previus block
        char empty_string[BF_BLOCK_SIZE];
        memset((*data), 0, BF_BLOCK_SIZE);

        // copy the first i-1
        memcpy((*data), temp, offset);

        // copy the values
        switch (openIndexes[indexArray]->attrType1) {
            case 'f':
                sprintf((*data)+offset, "%.1f", *(float *)value1);
                break;
            case 'i':
                sprintf((*data)+offset, "%d", *(int *)value1);
                break;
            case 'c':
                memcpy((*data)+offset, (char *)value1, openIndexes[indexArray]->attrLength1);
                break;
        }

        offset += openIndexes[indexArray]->attrLength1;

        switch (openIndexes[indexArray]->attrType2) {
            case 'f':
                sprintf((*data)+offset, "%.1f", *(float *)value2);
                break;
            case 'i':
                sprintf((*data)+offset, "%d", *(int *)value2);
                break;
            case 'c':
                memcpy((*data)+offset, (char *)value2, openIndexes[indexArray]->attrLength2);
                break;
        }

        offset += openIndexes[indexArray]->attrLength2;

        // copy the i+1 -> n
        memcpy((*data)+offset, temp + offset - size_of_entry, BF_BLOCK_SIZE - offset);

        // copy the pointer
        memcpy((*data)+BF_BLOCK_SIZE-4, temp+BF_BLOCK_SIZE-4, sizeof(int));

        // change the number of entries
        (*entries_counter)++;
        sprintf(BF_Block_GetData((*block))+5, "%d", (*entries_counter));

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));
    }
    // if the block is full
    else{
        // copy all the data of the block
        char temp[BF_BLOCK_SIZE  + size_of_entry];

        //store previous data
        memcpy(temp, (*data), offset);

        //store new entry
        switch (openIndexes[indexArray]->attrType1) {
            case 'f':
                sprintf(temp+offset, "%.1f", *(float *)value1);
                break;
            case 'i':
                sprintf(temp+offset, "%d", *(int *)value1);
                break;
            case 'c':
                memcpy(temp+offset, (char *)value1, openIndexes[indexArray]->attrLength1);
                break;
        }
        offset += openIndexes[indexArray]->attrLength1;

        switch (openIndexes[indexArray]->attrType2) {
            case 'f':
                sprintf(temp+offset, "%.1f", *(float *)value2);
                break;
            case 'i':
                sprintf(temp+offset, "%d", *(int *)value2);//!
                break;
            case 'c':
                memcpy(temp+offset, (char *)value2, openIndexes[indexArray]->attrLength2);
                break;
        }
        offset += openIndexes[indexArray]->attrLength2;

        //store the rest of the block
        memcpy(temp+offset, (*data)+offset-size_of_entry, BF_BLOCK_SIZE + size_of_entry - offset);

        // empty previus block
        char empty_string[BF_BLOCK_SIZE];
        memset((*data), 0, BF_BLOCK_SIZE);

        //store metadata on previus block
        store_metadata(data, block, block_num, 'l', ((*entries_counter)+1)/2, &offset);

        // store half of temp at the previus block
        memcpy((*data)+offset, temp+offset, (((*entries_counter)+1)*size_of_entry)/2);

        // allocate another block and get the data
        BF_Block* block2;
        BF_Block_Init(&block2);
        char* data2;
        CALL_OR_DIE(BF_AllocateBlock(openIndexes[indexArray]->fileDesc, block2));
        data2 = BF_Block_GetData(block2);
        int offset2 = 0;

        blocks_num++;
        // store the metadata in the second block
        store_metadata(&data2, &block2, blocks_num-1, 'l', ((*entries_counter)+1)/2, &offset2);

        // store the second half at the new block
        memcpy(data2+offset2, temp+size_of_metadata+((*entries_counter)+1)*size_of_entry/2, BF_BLOCK_SIZE + size_of_entry - size_of_metadata-((*entries_counter)+1)*size_of_entry/2);

        memcpy(temp_int, (temp)+size_of_entry+BF_BLOCK_SIZE-4, 4);

        //pointer from first to second block
        sprintf((*data)+BF_BLOCK_SIZE-size_of_pointer, "%d", blocks_num-1);

        // pointer from second to next block
        memcpy(data2+BF_BLOCK_SIZE-size_of_pointer, temp_int, size_of_pointer);

        // make new indexes
        sort_tree(block, &block2, stack, indexArray, blocks_num);

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));

        BF_Block_SetDirty(block2);
        CALL_OR_DIE(BF_UnpinBlock(block2));

    }

    return 1;
}

int go_down_tree(char** data, void* value1, char block_type, int *entries_counter, int indexArray, BF_Block** block, int* block_num, int* offset, Stack* stack){
    char temp_int[4], temp_float[8];
    int left_pointer, right_pointer;
    int entry_in_block=1;

    // while we are not at data level
    while (block_type != 'l'){
        entry_in_block = 1;

        // loop until we find greater key_value
        push(stack, *block_num);
        while(1){

            // store the pointer before the keyvalue
            memcpy(temp_int, (*data)+(*offset), sizeof(int));
            left_pointer = atoi(temp_int);
            (*offset) += sizeof(int);

            // take the left pointer if the keyvalues have finished
            if(entry_in_block>(*entries_counter)){

                BF_UnpinBlock((*block));
                //here left_pointer is actual the right pointer
                CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, left_pointer, (*block)));
                break;
            }

            if(openIndexes[indexArray]->attrType1 == 'f'){
                memcpy(temp_float, (*data)+(*offset), openIndexes[indexArray]->attrLength1);
                float key_value = atof(temp_float);

                // if we found greater key go deep in the tree
                if(*(float *)value1 <= key_value){
                    BF_UnpinBlock((*block));
                    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, left_pointer, (*block)));
                    break;
                }
            }
            else if(openIndexes[indexArray]->attrType1 == 'i'){
                memcpy(temp_int, (*data)+(*offset), openIndexes[indexArray]->attrLength1);
                int key_value = atoi(temp_int);

                // if we found greater key go deep in the tree
                if( *(int *) value1 <= key_value){
                    BF_UnpinBlock((*block));
                    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, left_pointer, (*block)));
                    break;
                }
            }
            else if(openIndexes[indexArray]->attrType1 == 'c'){
                char key_value[40];
                memcpy(key_value, (*data)+(*offset), openIndexes[indexArray]->attrLength1);

                // if we found greater key go deep in the tree
                int cmp = strcmp((char *)value1, key_value);
                if( cmp <= 0){
                    BF_UnpinBlock((*block));
                    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, left_pointer, (*block)));
                    break;
                }
            }

            (*offset) += openIndexes[indexArray]->attrLength1;
            entry_in_block++;
        }
        // get the metadata of the next block
        get_metadata(data, block, block_num, &block_type, entries_counter, offset);
    }
    return 1;
}

int sort_tree(BF_Block **block, BF_Block **block2, Stack* stack, int indexArray, int blocks_num){

    int offset = 0, offset2 = 0, entries_counter, block_num;
    char block_type, *data;

    const int size_of_entry = openIndexes[indexArray]->attrLength1 + openIndexes[indexArray]->attrLength2;
    const int size_of_indexpair = openIndexes[indexArray]->attrLength1 + sizeof(int);
    const int size_of_pointer = sizeof(int);

    // if there is not upper level or we split the root
    if(isEmpty(stack)){
        BF_Block *root_block;
        BF_Block_Init(&root_block);
        char* root_data;

        CALL_OR_DIE(BF_AllocateBlock(openIndexes[indexArray]->fileDesc, root_block));
        root_data = BF_Block_GetData(root_block);

        // store the metadata in the root block
        blocks_num++;
        store_metadata(&root_data, &root_block, blocks_num-1, 'i', 1, &offset);

        // take the metadata from the 1 out of 2 blocks we created
        get_metadata(&data, block, &block_num, &block_type, &entries_counter, &offset2);

        // make the first pointer
        sprintf(root_data+offset, "%d", block_num);
        offset += size_of_pointer;

        // store the keyvalue
        if(block_type == 'l'){
            memcpy(root_data+offset, data+(entries_counter-1)*size_of_entry+offset2, openIndexes[indexArray]->attrLength1);
            offset += openIndexes[indexArray]->attrLength1;
        }
        else if(block_type == 'i'){

            memcpy(root_data+offset, data+(entries_counter-1)*size_of_indexpair+offset2+size_of_pointer, openIndexes[indexArray]->attrLength1);
            offset += openIndexes[indexArray]->attrLength1;
        }

        //get the metadata of the second block
        get_metadata(&data, block2, &block_num, &block_type, &entries_counter, &offset2);

        // store the pointer to the second block
        sprintf(root_data+offset, "%d", block_num);

        BF_Block_SetDirty(root_block);
        CALL_OR_DIE(BF_UnpinBlock(root_block));

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));

        BF_Block_SetDirty(*block2);
        CALL_OR_DIE(BF_UnpinBlock(*block2));

        //store the new root in first block of file with metadata
        CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, 0, *block));
        data = BF_Block_GetData(*block);

        sprintf(data+10, "%d", blocks_num-1);

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));
    }
    //if there is already upper level
    else{
        char *data;
        char key_value[40];

        //take the last keyvalue of the left new block
        get_metadata(&data, block, &block_num, &block_type, &entries_counter, &offset);

        if(block_type == 'l')
            memcpy(key_value, data+offset+(entries_counter-1)*size_of_entry, openIndexes[indexArray]->attrLength1);
        else
            memcpy(key_value, data+offset+(entries_counter-1)*size_of_indexpair+size_of_pointer, openIndexes[indexArray]->attrLength1);

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));

        BF_Block_SetDirty(*block2);
        CALL_OR_DIE(BF_UnpinBlock(*block2));

        // go to the father of left block to insert the index to the new block
        CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, pop(stack), *block));
        data = BF_Block_GetData(*block);

        insert_index(&data, key_value, indexArray, block, blocks_num, stack);
    }

    return AME_OK;
}

int insert_index(char** data, void* value1, int indexArray, BF_Block** block, int blocks_num, Stack* stack){
    char temp_int[4], temp_float[8], counter[4];
    int offset, entries_counter, block_num, eq = 0, entry_in_block = 1;
    char block_type;

    const int size_of_indexpair = openIndexes[indexArray]->attrLength1 + sizeof(int);
    const int size_of_metadata = sizeof(block_num)+sizeof(block_type)+sizeof(entries_counter);
    const int size_of_pointer = sizeof(int);

    memcpy(temp_float, value1, openIndexes[indexArray]->attrLength1);

    get_metadata(data, block, &block_num, &block_type, &entries_counter, &offset);
    offset += sizeof(int);

    // find a greater value inside the block
    while(1){
        if(openIndexes[indexArray]->attrType1 == 'f'){
            // copy key_value to compare
            memcpy(temp_float, (*data)+offset, openIndexes[indexArray]->attrLength1);
            float key_value = atof(temp_float);

            // if we found greater key stop
            if(atof((char *)value1) <= key_value){
                if(atof((char *)value1) == key_value) eq = 1;
                break;
            }

        }
        else if(openIndexes[indexArray]->attrType1 == 'i'){
            // copy key_value to compare
            memcpy(temp_int, (*data)+offset, openIndexes[indexArray]->attrLength1);
            int key_value = atoi(temp_int);

            // if we found greater key stop
            if( atoi((char *)value1) <= key_value){
                if( atoi((char *)value1) == key_value) eq = 1;
                break;
            }
        }
        else if(openIndexes[indexArray]->attrType1 == 'c'){
            // copy key_value to compare
            char key_value[40];
            memcpy(key_value, (*data)+offset, openIndexes[indexArray]->attrLength1);

            // if we found greater key stop
            int cmp = strcmp((char *)value1, key_value);
            if( cmp <= 0){
                if (cmp == 0) eq = 1;
                break;
            }
        }

        if (entry_in_block>entries_counter) break;
        offset+=size_of_indexpair;
        entry_in_block++;

    }
    // if the block has space for another index
    if(entries_counter < ((BF_BLOCK_SIZE-size_of_metadata-size_of_pointer)/size_of_indexpair)){

        char temp[BF_BLOCK_SIZE];
        memcpy(temp, (*data), BF_BLOCK_SIZE);

        // empty previus block
        char empty_string[BF_BLOCK_SIZE];
        memset((*data), 0, BF_BLOCK_SIZE);

        // copy the first i-1
        memcpy((*data), temp, offset);

        memcpy((*data)+offset, value1, openIndexes[indexArray]->attrLength1);
        offset += openIndexes[indexArray]->attrLength1;

        // copy the pointer
        sprintf((*data)+offset, "%d", blocks_num-1);
        offset +=sizeof(int);

        // copy the i+1 -> n
        memcpy((*data)+offset, temp + offset - size_of_indexpair, BF_BLOCK_SIZE - offset);

        // change the number of entries
        entries_counter++;
        sprintf(BF_Block_GetData((*block))+5, "%d", entries_counter);

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));
    }
    // if the index block is full
    else{
        // copy all the data of the block
        char temp[BF_BLOCK_SIZE  + size_of_indexpair];

        //store previous data
        memcpy(temp, (*data), offset);

        //store new entry
        switch (openIndexes[indexArray]->attrType1) {
            case 'f':
                sprintf(temp+offset, "%.1f", *(float *)value1);
                break;
            case 'i':
                sprintf(temp+offset, "%d", *(int *)value1);
                break;
            case 'c':
                memcpy(temp+offset, (char *)value1, openIndexes[indexArray]->attrLength1);
                break;
        }

        offset += openIndexes[indexArray]->attrLength1;

        // copy the pointer
        sprintf((temp)+offset, "%d", blocks_num-1);
        offset += sizeof(int);

        //store the rest of the block
        memcpy(temp+offset, (*data)+offset-size_of_indexpair, BF_BLOCK_SIZE + size_of_indexpair - offset);

        // empty previus block
        char empty_string[BF_BLOCK_SIZE];
        memset((*data), 0, BF_BLOCK_SIZE);

        //store metadata on previus block
        store_metadata(data, block, block_num, 'i', ((entries_counter)+1)/2, &offset);

        // store half of temp at the previus block
        memcpy((*data)+offset, temp+offset, (((entries_counter)+1)*size_of_indexpair)/2);

        // allocate another block and get the data
        BF_Block* block2;
        BF_Block_Init(&block2);
        char* data2;
        CALL_OR_DIE(BF_AllocateBlock(openIndexes[indexArray]->fileDesc, block2));
        data2 = BF_Block_GetData(block2);
        int offset2 = 0;

        blocks_num++;
        // store the metadata in the second block
        store_metadata(&data2, &block2, blocks_num-1, 'i', ((entries_counter)+1)/2, &offset2);

        // store the second half at the new block
        memcpy(data2+offset2, temp+9+((entries_counter)+1)*size_of_indexpair/2, BF_BLOCK_SIZE + size_of_indexpair - size_of_metadata -((entries_counter)+1)*size_of_indexpair/2);

        // make new indexes
        sort_tree(block, &block2, stack, indexArray, blocks_num);

        BF_Block_SetDirty(*block);
        CALL_OR_DIE(BF_UnpinBlock(*block));

        BF_Block_SetDirty(block2);
        CALL_OR_DIE(BF_UnpinBlock(block2));
    }

}

int set_first_block(int indexArray, void *value1, void *value2){
    BF_Block *block1, *block2;
    BF_Block_Init(&block1);
    BF_Block_Init(&block2);
    int offset = 0;
    char* data;
    CALL_OR_DIE(BF_AllocateBlock(openIndexes[indexArray]->fileDesc, block1));
    data = BF_Block_GetData(block1);

    // set the block number
    sprintf(data, "%d", 1);
    //memcpy(data, "1", sizeof(int));

    // set leaf or index
    offset += sizeof(int);
    memcpy(data+offset, "l", sizeof(char));

    // set entry counter
    offset += sizeof(char);
    //memcpy(data+offset, "1", sizeof(int));
    sprintf(data+offset, "%d", 1);

    // set value1
    offset += sizeof(int);
    switch (openIndexes[indexArray]->attrType1) {
        case 'f':
            sprintf(data+offset, "%.1f", *(float *)value1);//!
            printf("%f\n", atof(data+offset));
            break;
        case 'i':
            sprintf(data+offset, "%d", *(int *)value1);//!
            break;
        case 'c':
            memcpy(data+offset, (char *)value1, openIndexes[indexArray]->attrLength1);
            break;
    }

    // set value2
    offset += openIndexes[indexArray]->attrLength1;
    switch (openIndexes[indexArray]->attrType2) {
        case 'f':
            sprintf(data+offset, "%.1f", *(float *)value2);//!
            break;
        case 'i':
            sprintf(data+offset, "%d", *(int *)value2);//!
            break;
        case 'c':
            memcpy(data+offset, (char *)value2, openIndexes[indexArray]->attrLength2);
            break;
    }

    // set pointer to next (nothing)
    offset += openIndexes[indexArray]->attrLength2;
    sprintf(data+BF_BLOCK_SIZE-4, "%d", -1);

    // set the block as root and store root block at metadata
    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, 0, block2));
    data = BF_Block_GetData(block2);
    sprintf(data+10, "%d", 1);

    BF_Block_SetDirty(block1);
    CALL_OR_DIE(BF_UnpinBlock(block1));

    BF_Block_SetDirty(block2);
    CALL_OR_DIE(BF_UnpinBlock(block2));

    return AME_OK;
}

// int print(indexArray){
//     printf("\nIn Print..\n");
//     int fd;
//     BF_Block *block;
//     char *data, counter[4], counter2[8], block_type, temp[40];
//     int offset, of = 9, block_num, blocks_num, entries_counter;
//     BF_Block_Init(&block);
//     CALL_OR_DIE(BF_GetBlockCounter(openIndexes[indexArray]->fileDesc, &blocks_num));
//     CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, 0, block));
//     data = BF_Block_GetData(block);
//     memcpy(counter, data+10, sizeof(int));
//     printf("Root Node %d\n", atoi(counter));
//
//     CALL_OR_DIE(BF_UnpinBlock(block));
//
//     for (int i = 1; i < blocks_num; i++) {
//         of = 9;
//         CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, i, block));
//
//         get_metadata(&data, &block, &block_num, &block_type, &entries_counter, &offset);
//
//         printf("Number of blocks in file %d Block Type %c Entries counter %d block num %d\n", blocks_num, block_type, entries_counter, block_num);
//
//         if(block_type == 'l'){
//                for (int j = 0; j < entries_counter; j++) {
//                     switch (openIndexes[indexArray]->attrType1) {
//                         case 'f':
//                             memcpy(counter2, data+of, openIndexes[indexArray]->attrLength1);
//                             printf("%.1f\n", atof(counter2));
//                             break;
//                         case 'i':
//                            memcpy(counter, data+of, openIndexes[indexArray]->attrLength1);
//                            printf("%d\n", atoi(counter));
//                            break;
//                         case 'c':
//                             memcpy(temp, data+of, openIndexes[indexArray]->attrLength1);
//                             printf("%s\n", temp);
//                             break;
//                    }
//
//
//                    of += openIndexes[indexArray]->attrLength1;
//
//                    switch (openIndexes[indexArray]->attrType2) {
//                        case 'f':
//                            memcpy(counter2, data+of, openIndexes[indexArray]->attrLength2);
//                            printf("%.1f\n", atof(counter2));
//                            break;
//                        case 'i':
//                           memcpy(counter, data+of, openIndexes[indexArray]->attrLength2);
//                           printf("%d\n", atoi(counter));
//                           break;
//                        case 'c':
//                            memcpy(temp, data+of, openIndexes[indexArray]->attrLength2);
//                            printf("%s\n", temp);
//                            break;
//                   }
//
//                    of += openIndexes[indexArray]->attrLength2;
//                }
//                memcpy(counter, data+BF_BLOCK_SIZE-4, 4);
//                printf("%d\n", atoi(counter));
//         }
//         else if(block_type == 'i'){
//             get_metadata(&data, &block, &block_num, &block_type, &entries_counter, &offset);
//             printf("Block Type %c Entries counter %d block num %d blocks Num %d\n", block_type, entries_counter, block_num, blocks_num );
//             for (int k = 0; k < entries_counter; k++) {
//                 memcpy(counter, data+of, sizeof(int));
//                 printf("%d\n", atoi(counter));
//                 of += 4;
//                 switch (openIndexes[indexArray]->attrType1) {
//                     case 'f':
//                         memcpy(counter2, data+of, openIndexes[indexArray]->attrLength1);
//                         printf("%.1f\n", atof(counter2));
//                         break;
//                     case 'i':
//                        memcpy(counter, data+of, openIndexes[indexArray]->attrLength1);
//                        printf("%d\n", atoi(counter));
//                        break;
//                     case 'c':
//                         memcpy(temp, data+of, openIndexes[indexArray]->attrLength1);
//                         printf("%s\n", temp);
//                         break;
//                }
//                 of += openIndexes[indexArray]->attrLength1;
//             }
//
//             memcpy(counter, data+of, sizeof(int));
//             printf("%d\n", atoi(counter));
//         }
//         CALL_OR_DIE(BF_UnpinBlock(block));
//     }
// }
