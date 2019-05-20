#include "AM.h"

int AM_errno = AME_OK;
indexes** openIndexes;
scans** openScans;

void AM_Init() {
	BF_Init(LRU);
    openIndexes = malloc(20*sizeof(indexes));
    for (int i = 0; i < 20; i++) {
        openIndexes[i] = NULL;
    }

    openScans = malloc(20*sizeof(scans));
    for (int i = 0; i < 20; i++) {
        openScans[i] = NULL;
    }

	return;
}

int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {

    if(attrType1 == 'f' && attrLength1!=sizeof(float))
        return -90;
    if(attrType1 == 'i' && attrLength1!=sizeof(int))
        return -90;
    if(attrType2 == 'f' && attrLength2!=sizeof(float))
        return -90;
    if(attrType2 == 'i' && attrLength2!=sizeof(int))
        return -90;


	int fd;
    char *data;
    int offset = 0;
	BF_Block *block;
    CALL_OR_DIE (BF_CreateFile(fileName));
    CALL_OR_DIE (BF_OpenFile(fileName, &fd));

	// metadata block(0)
    BF_Block_Init(&block);
    BF_AllocateBlock(fd, block);
	data = BF_Block_GetData(block);

    // explained in readme
    if(attrType1 == 'f')
        attrLength1++;
    if(attrType2 == 'f')
        attrLength2++;

    // store the type of value1
    memcpy(data, &attrType1, sizeof(attrType1));
    offset += sizeof(attrType1);

    // store the length of value1
    sprintf(data+offset, "%d", attrLength1);
    offset += sizeof(attrLength1);

    // store the type of value2
    memcpy(data+offset, &attrType2, sizeof(attrType2));
    offset += sizeof(attrType2);

    // store the length of value 2
    sprintf(data+offset, "%d", attrLength2);
    offset += sizeof(attrLength2);

    //store the root of file(-1 at start)
    offset += sizeof(int);
    sprintf(data+offset, "%d", -1);

    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    CALL_OR_DIE(BF_CloseFile(fd));

  	return AME_OK;
}

int AM_DestroyIndex(char *fileName) {
    char c[100];
	int dir_len;

	getcwd(c, PATH_SIZE);

    dir_len = strlen(c);
    c[dir_len] = '/';
    strcpy(c + dir_len+1, fileName);
    if (remove(c) != 0)
        return -6;

    return AME_OK;
}

int AM_OpenIndex (char *fileName) {
    char *data;
    int i, fd;
    int offset=0;
    char counter[4];

    // open the file
    CALL_OR_DIE(BF_OpenFile(fileName, &fd));

    // get the right block
    BF_Block *block;
    BF_Block_Init(&block);
    BF_GetBlock(fd, 0, block);
	data = BF_Block_GetData(block);

    for ( i = 0; i < 20; i++) {

        if(openIndexes[i] == NULL){

            offset = 0;
            openIndexes[i] = malloc(sizeof(indexes));
            openIndexes[i]->fileDesc = fd;
            memcpy(&(openIndexes[i]->attrType1), data, sizeof(char));
            offset += sizeof(char);
            memcpy(counter, data+offset, sizeof(int));
            openIndexes[i]->attrLength1 = atoi(counter);

            offset += sizeof(int);
            memcpy(&(openIndexes[i]->attrType2), data+offset, sizeof(char));

            offset += sizeof(char);
            memcpy(counter, data+offset, sizeof(int));
            openIndexes[i]->attrLength2 = atoi(counter);

            break;
        }
    }
    if(i == 20)
        AM_errno = -7;

    CALL_OR_DIE(BF_UnpinBlock(block));

    return i;
}

int AM_CloseIndex (int indexArray) {
    if(openScans[openIndexes[indexArray]->fileDesc] == NULL){
        CALL_OR_DIE(BF_CloseFile(openIndexes[indexArray]->fileDesc));
        free(openIndexes[indexArray]);
        openIndexes[indexArray] = NULL;
        return AME_OK;
    }
    else{
        AM_errno = -25;
        return AM_errno;
    }

}

// B+ block ------> |block #|leaf_or_index|entries_counter|pointer|value|...|next_block|
// Block of data -> |block #|leaf_or_index|entries_counter|value1|value2|...|next_block|
int AM_InsertEntry(int indexArray, void *value1, void *value2) {

    BF_Block* block;
    BF_Block_Init(&block);
    char* data;
    char block_type, temp_int[4], temp_float[8], temp_char;
    int blocks_num, block_num;
    int pointers_number, entries_counter;
    int offset = 0, eq = 0;
    int size_of_entry = openIndexes[indexArray]->attrLength1 + openIndexes[indexArray]->attrLength2;

    CALL_OR_DIE(BF_GetBlockCounter(openIndexes[indexArray]->fileDesc, &blocks_num));

    // if there is only the metadata block, make the root
    if (blocks_num == 1){
        set_first_block(indexArray, value1, value2);
        return AME_OK;
    }

    //Search the B+Tree

    //stack initialization
    struct Stack* stack = createStack(STACK_SIZE);

    //find the root of tree whics is stored at the metadata of the file
    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, 0, block));
    data = BF_Block_GetData(block);
    memcpy(temp_int, data+10, sizeof(int));

    CALL_OR_DIE(BF_UnpinBlock(block));

    CALL_OR_DIE(BF_GetBlock(openIndexes[indexArray]->fileDesc, atoi(temp_int), block));
    data = BF_Block_GetData(block);

    // get data until the first pointer
    get_metadata(&data, &block, &block_num, &block_type, &entries_counter, &offset);

    // go down the tree
    AM_errno = go_down_tree(&data, value1, block_type, &entries_counter, indexArray, &block, &block_num, &offset, stack);

    //we reached the leaves insert the values
    AM_errno = insert_node(&data, value1, value2, &entries_counter, indexArray, &block, blocks_num, block_num, offset, stack);

    return AME_OK;
}

int AM_OpenIndexScan(int fileDesc, int op, void *value) {

    char *data;
    int i, pointer, block_num, entries_counter;
    int offset=0;
    char counter[4], block_type;

    for (i = 0; i < 20; i++) {
        if(openScans[i] == NULL){
            openScans[i] = malloc(sizeof(scans));
            break;
        }
    }

    if(i == 20)
        AM_errno = -8;

    BF_Block* block;
    //find the root in the first block of the file with the metadata
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(openIndexes[fileDesc]->fileDesc, 0, block));
    data = BF_Block_GetData(block);

    offset += 10;
    CALL_OR_DIE(BF_UnpinBlock(block));

    //take the metadata of the root node
    CALL_OR_DIE(BF_GetBlock(openIndexes[fileDesc]->fileDesc, atoi(data+offset), block));
    get_metadata(&data, &block, &block_num, &block_type, &entries_counter, &offset);

    //while we are in index block
    while(block_type == 'i'){

        //take the most left pointer of the index block
        pointer = atoi(data+offset);

        //leave the block
        CALL_OR_DIE(BF_UnpinBlock(block));

        //now take the block that points the left pointer
        CALL_OR_DIE(BF_GetBlock(openIndexes[fileDesc]->fileDesc, pointer, block));
        get_metadata(&data, &block, &block_num, &block_type, &entries_counter, &offset);
    }

    openScans[i]->fileDesc = openIndexes[fileDesc]->fileDesc;

    openScans[i]->block_num = block_num;

    openScans[i]->record_in_block = 0;

    openScans[i]->entries_counter = entries_counter;

    openScans[i]->op = op;

    switch (openIndexes[fileDesc]->attrType1) {
        case 'f':
            openScans[i]->value = (float *)malloc(openIndexes[fileDesc]->attrLength1);
            *(float *)openScans[i]->value = *(float *)value;
            switch (openIndexes[fileDesc]->attrType2) {
                case 'f':
                    openScans[i]->returning_value = (float *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'i':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'c':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
            }
            break;

        case 'i':
            openScans[i]->value = (int *)malloc(openIndexes[fileDesc]->attrLength1);
            *(int *)openScans[i]->value = *(int *)value;
            switch (openIndexes[fileDesc]->attrType2) {
                case 'f':
                    openScans[i]->returning_value = (float *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'i':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'c':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
            }
            break;
        case 'c':
            openScans[i]->value = (char *)malloc(openIndexes[i]->attrLength1);
            strcpy((char *)openScans[i]->value, (char *)value);
            switch (openIndexes[fileDesc]->attrType2) {
                case 'f':
                    openScans[i]->returning_value = (float *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'i':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
                case 'c':
                    openScans[i]->returning_value = (int *)malloc(openIndexes[fileDesc]->attrLength2);
                    break;
            }
            break;
        }

    CALL_OR_DIE(BF_UnpinBlock(block));
    return i;
}

void *AM_FindNextEntry(int scanDesc) {

    char *data;
    BF_Block* block;
    BF_Block_Init(&block);
    int offset = 0;
    char temp[4], block_type;
    char temp_float[8];
    char temp_char[40];
    int next_pointer;
    char* value = malloc(openIndexes[openScans[scanDesc]->fileDesc]->attrLength2);
    int size_of_entry = openIndexes[openScans[scanDesc]->fileDesc]->attrLength1 + openIndexes[openScans[scanDesc]->fileDesc]->attrLength2;
    int curr_block_num = openScans[scanDesc]->block_num;

    BF_GetBlock(openScans[scanDesc]->fileDesc, openScans[scanDesc]->block_num, block);
    data = BF_Block_GetData(block);
    get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);

    offset += (openScans[scanDesc]->record_in_block)*size_of_entry;

    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

        //take the pointer to next block
        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

        // if it was the last entry of the last block
        if(next_pointer == -1){
            AM_errno = AME_EOF;
            printf("Search ends here\n");
            BF_UnpinBlock(block);
            free(value);
            return NULL;
        }
        // else go to the next block
        else{
            BF_UnpinBlock(block);
            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
            data = BF_Block_GetData(block);
            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
            openScans[scanDesc]->block_num = next_pointer;
            openScans[scanDesc]->record_in_block=0;
        }
    }

    memcpy(temp_char, data+offset, openIndexes[openScans[scanDesc]->fileDesc]->attrLength1);

    //key_value op value
    if(openIndexes[openScans[scanDesc]->fileDesc]->attrType1 == 'f'){

        float key_value = atof(temp_char);

        switch (openScans[scanDesc]->op) {
            case EQUAL:
                // while we have not found an not equal key_value
                while (key_value != *(float *)openScans[scanDesc]->value){

                    offset += size_of_entry;
                    key_value = atof(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));
                        // printf("next block : %d\n", next_pointer);

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atof(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case NOT_EQUAL:

                // while we have not found an not equal key_value
                while (key_value == *(float *)openScans[scanDesc]->value){

                    offset += size_of_entry;
                    key_value = atof(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");

                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atof(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN:

                if(key_value >= *(float *)openScans[scanDesc]->value){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    BF_UnpinBlock(block);
                    free(value);
                    return NULL;
                }
                break;
            case GREATER_THAN:

                while (key_value <= *(float *)openScans[scanDesc]->value){
                    offset += size_of_entry;
                    key_value = atof(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atof(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN_OR_EQUAL:

                if(key_value > *(float *)openScans[scanDesc]->value){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    BF_UnpinBlock(block);
                    free(value);
                    return NULL;
                }
                break;
            case GREATER_THAN_OR_EQUAL:

                while (key_value < *(float *)openScans[scanDesc]->value){
                    offset += size_of_entry;
                    key_value = atof(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atof(data+offset);
                            continue;
                        }
                    }
                }
        }
    }
    else if(openIndexes[openScans[scanDesc]->fileDesc]->attrType1 == 'i'){
        int key_value = atoi(temp_char);

        switch (openScans[scanDesc]->op) {
            case EQUAL:

                // while we have not found an not equal key_value
                while (key_value != *(int *)openScans[scanDesc]->value){

                    offset += size_of_entry;
                    key_value = atoi(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atoi(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case NOT_EQUAL:

                // while we have not found an not equal key_value
                while (key_value == *(int *)openScans[scanDesc]->value){

                    offset += size_of_entry;
                    key_value = atoi(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atoi(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN:

                if(key_value >= *(int *)openScans[scanDesc]->value){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    BF_UnpinBlock(block);
                    free(value);
                    return NULL;
                }
                break;
            case GREATER_THAN:

                while (key_value <= *(int *)openScans[scanDesc]->value){
                    offset += size_of_entry;
                    key_value = atoi(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atoi(data+offset);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN_OR_EQUAL:

                if(key_value > *(int *)openScans[scanDesc]->value){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    BF_UnpinBlock(block);
                    free(value);
                    return NULL;
                }
                break;
            case GREATER_THAN_OR_EQUAL:

                while (key_value < *(int *)openScans[scanDesc]->value){
                    offset += size_of_entry;
                    key_value = atoi(data+offset);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            key_value = atoi(data+offset);
                            continue;
                        }
                    }
                }
                break;
        }
    }
    else if(openIndexes[openScans[scanDesc]->fileDesc]->attrType1 == 'c'){

        char key_value[40];
        strcpy(key_value, temp_char);

        switch (openScans[scanDesc]->op) {
            case EQUAL:

                // while we have not found an not equal key_value
                while (strcmp(key_value, (char*)openScans[scanDesc]->value)!=0){

                    offset += size_of_entry;
                    memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                            continue;
                        }
                    }
                }
                break;
            case NOT_EQUAL:

                // while we have not found an not equal key_value
                while (strcmp(key_value, (char*)openScans[scanDesc]->value)==0){

                    offset += size_of_entry;
                    memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN:

                if(strcmp(key_value, (char*)openScans[scanDesc]->value)>=0){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    free(value);
                    BF_UnpinBlock(block);
                    return NULL;
                }
                break;
            case GREATER_THAN:
                // while we have not found an not equal key_value
                while (strcmp(key_value, (char*)openScans[scanDesc]->value)<=0){

                    offset += size_of_entry;
                    memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                            continue;
                        }
                    }
                }
                break;
            case LESS_THAN_OR_EQUAL:

                if(strcmp(key_value, (char*)openScans[scanDesc]->value)>0){
                    AM_errno = AME_EOF;
                    printf("Search ends here\n");
                    free(value);
                    BF_UnpinBlock(block);
                    return NULL;
                }
                break;
            case GREATER_THAN_OR_EQUAL:
                // while we have not found an not equal key_value
                while (strcmp(key_value, (char*)openScans[scanDesc]->value)<0){

                    offset += size_of_entry;
                    memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                    openScans[scanDesc]->record_in_block++;

                    //if we reached the end of the current block
                    if(openScans[scanDesc]->record_in_block == openScans[scanDesc]->entries_counter){

                        //take the pointer to next block
                        next_pointer = atoi(data + BF_BLOCK_SIZE - sizeof(int));

                        // if it was the last entry of the last block
                        if(next_pointer == -1){
                            AM_errno = AME_EOF;
                            printf("Search ends here\n");
                            BF_UnpinBlock(block);
                            free(value);
                            return NULL;
                        }
                        // else go to the next block
                        else{

                            BF_UnpinBlock(block);
                            BF_GetBlock(openScans[scanDesc]->fileDesc, next_pointer, block);
                            data = BF_Block_GetData(block);
                            get_metadata(&data, &block, &curr_block_num, &block_type, &(openScans[scanDesc]->entries_counter), &offset);
                            openScans[scanDesc]->block_num = next_pointer;
                            openScans[scanDesc]->record_in_block=0;
                            memcpy(key_value, data+offset, openIndexes[scanDesc]->attrLength1);
                            continue;
                        }
                    }
                }
        }
    }

    openScans[scanDesc]->record_in_block++;

    BF_UnpinBlock(block);
    offset += openIndexes[openScans[scanDesc]->fileDesc]->attrLength1;
    memcpy(value, data+offset, openIndexes[openScans[scanDesc]->fileDesc]->attrLength2);
    switch (openIndexes[openScans[scanDesc]->fileDesc]->attrType2) {
        case 'f':
            *(float *)openScans[scanDesc]->returning_value = atof(value);
            free(value);
            return (float *)openScans[scanDesc]->returning_value;
        case 'i':
            *(int *)openScans[scanDesc]->returning_value = atoi(value);
            free(value);
            return (int *)openScans[scanDesc]->returning_value;
        case 'c':
            strcpy((char *)openScans[scanDesc]->returning_value, value);
            free(value);
            return (char *)openScans[scanDesc]->returning_value;
    }
}

int AM_CloseIndexScan(int scanDesc) {

    free(openScans[scanDesc]->value);
    openScans[scanDesc]->value = NULL;

    free(openScans[scanDesc]->returning_value);
    openScans[scanDesc]->returning_value = NULL;

    free(openScans[scanDesc]);
    openScans[scanDesc] = NULL;

    return AME_OK;
}

void AM_PrintError(char *errString) {
    printf("%s\n", errString);
    if(AM_errno == -5){
        printf("Error at BF level\n");
    }
    else if(AM_errno == -6){
        printf("\n");
    }

}

void AM_Close() {

    // free the memory allocated for each struct
    for (size_t i = 0; i < 20; i++) {
        if(openIndexes[i] != NULL){
            free(openIndexes[i]);
            openIndexes[i] = NULL;
        }
    }

    BF_Close();

    // free the memory allocated for the array
    free(openIndexes);
}
