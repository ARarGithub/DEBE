/**
 * @file dataWriter.cc
 * @author Zuoru YANG (zryang@cse.cuhk.edu.hk)
 * @brief implement the interface defined in data writer
 * @version 0.1
 * @date 2020-02-01
 * 
 * @copyright Copyright (c) 2020
 * 
 */

#include "../../include/dataWriter.h"

extern Configure config;

struct timeval sTimeDataWrite;
struct timeval eTimeDataWrite;
struct timeval sTotalTime;
struct timeval eTotalTime;

DataWriter::DataWriter() {
    containerNamePrefix_ = config.GetContainerRootPath();
    containerNameTail_ = config.GetContainerSuffix();
    tool::Logging(myName_.c_str(), "init the DataWriter.\n");
}

/**
 * @brief Destroy the Data Writer object
 * 
 */
DataWriter::~DataWriter() {
    fprintf(stderr, "========DataWriter Info========\n");
#if (DATAWRITER_BREAKDOWN == 1)
    fprintf(stderr, "write container time: %lf\n", writeTime_);
#endif
    fprintf(stderr, "writer container num: %lu\n", containerNum_);
    fprintf(stderr, "===============================\n");
}

/**
 * @brief the main process of data writer
 * 
 * @param inputMQ the input MQ
 */
void DataWriter::Run(MessageQueue<Container_t>* inputMQ) {
    bool jobDoneFlag = false;

    // store the container extract from the MQ
    Container_t tmpContainer;

    tool::Logging(myName_.c_str(), "the main thread is running.\n");
    gettimeofday(&sTotalTime, NULL);
    while (true) {
        // the main loop        
        if (inputMQ->done_ && inputMQ->IsEmpty()) {
            jobDoneFlag = true;
        }

        if (inputMQ->Pop(tmpContainer)) {
            // write this container to the disk.
#if (DATAWRITER_BREAKDOWN == 1)
            gettimeofday(&sTimeDataWrite, NULL);
#endif
            SaveToFile(tmpContainer);
#if (DATAWRITER_BREAKDOWN == 1)
            gettimeofday(&eTimeDataWrite, NULL);
            writeTime_ += tool::GetTimeDiff(sTimeDataWrite, eTimeDataWrite);
#endif
            containerNum_++;
        }

        if (jobDoneFlag) {
            break;
        }
        
    }

    gettimeofday(&eTotalTime, NULL);
    totalTime_ += tool::GetTimeDiff(sTotalTime, eTotalTime);

    tool::Logging(myName_.c_str(), "thread exit.\n");
    return ;
}


/**
 * @brief Save the container data to a physical file on disk
 *
 * @param newContainer the container to be saved, containing ID and data
 */
void DataWriter::SaveToFile(Container_t& newContainer) {
    FILE* containerFile = NULL;  // File pointer for the container file

    // Create filename from container ID (binary to string conversion)
    string fileName((char*)newContainer.containerID, CONTAINER_ID_LENGTH);

    // Construct full path by combining:
    // - container root path (prefix)
    // - container ID (filename)
    // - container suffix (file extension)
    string fileFullName = containerNamePrefix_ + fileName + containerNameTail_;

    // Open file in binary write mode
    containerFile = fopen(fileFullName.c_str(), "wb");
    if (!containerFile) {
        // Error handling if file cannot be opened
        tool::Logging(myName_.c_str(), "cannot open container file: %s\n", fileFullName.c_str());
        exit(EXIT_FAILURE);
    }

    // Write container data to file:
    // - newContainer.body: pointer to data buffer
    // - newContainer.currentSize: size of data to write
    // - 1: number of elements to write
    // - containerFile: destination file
    fwrite((char*)newContainer.body, newContainer.currentSize, 1, containerFile);

    // Close the file
    fclose(containerFile);
    return ;
}