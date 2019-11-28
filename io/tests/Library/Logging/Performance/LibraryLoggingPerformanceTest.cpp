#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <ctime>
#include <mutex>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <ostream>
#include <functional>
#include <boost/asio.hpp>
#include "Library/Logging/CoutOutput.h"
#include "Library/Logging/Logger.h"
#include "Library/Logging/ServiceLogging.h"
#include "Library/Logging/TcpOutput.h"
#include "Library/Network/GenericSocket.h"


struct Server {
    Server(const std::string& host, const std::string& port, unsigned int bufferSize)
    : host {host}, port {port}, bufferSize {bufferSize}
    { }

    void start() {
        std::packaged_task<unsigned int()> task(std::bind(&Server::run, this));

        future = task.get_future();

        std::thread(std::move(task)).detach();

        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    unsigned int getBytesReceived() {
        future.wait();
        return future.get();
    }

    unsigned int run() {
        using namespace boost::asio;

        io_service service;
        ip::tcp::socket socket(service);
        ip::tcp::acceptor acceptor(service, ip::tcp::endpoint(ip::address::from_string(host), (short) std::stoul(port)));

        acceptor.accept(socket);

        unsigned int counter {0};
        char* buffer = new char[bufferSize];
        for (;;)
        {
            boost::system::error_code error;
            counter += socket.read_some(boost::asio::buffer(buffer, bufferSize), error);
            if (error == boost::asio::error::eof) {
                break;
            }
            else if (error) {
                std::cerr << boost::system::system_error(error).what() << std::endl;
                break;
            }
        }

        delete[] buffer;
        return counter;
    }

    const std::string host;
    const std::string port;
    const unsigned int bufferSize;
    std::future<unsigned int> future;
};


struct ClientSocket : public Library::Network::GenericSocket {
    ClientSocket()
    : socket(io_service) {
    }

    void connect(const char* host, const char* port) {
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(boost::asio::ip::tcp::v4(), host, port);
        boost::asio::ip::tcp::resolver::iterator iterator = resolver.resolve(query);
        boost::asio::connect(socket, iterator);
    }

    void close() {
        socket.close();
    }

    void write(std::string&& data) {
        totalPacketsSent++;
        totalBytesSent += boost::asio::write(socket, boost::asio::buffer(data));
    }

    void write(const std::string& data) {
        totalPacketsSent++;
        totalBytesSent += boost::asio::write(socket, boost::asio::buffer(data));
    }

    void setTotalBytesMessage(unsigned int value) {
        totalBytesMessage = value;
    }

    unsigned int getTotalBytesSent() {
        return totalBytesSent;
    }

    unsigned int getTotalPacketsSent() {
        return totalPacketsSent;
    }

    void wait() {
        int counter = 0;
        while (counter++ < 10) {
            if (totalPacketsSent == totalBytesMessage) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    unsigned int totalBytesSent {0};
    unsigned int totalPacketsSent {0};
    unsigned int totalBytesMessage {0};

    boost::asio::io_service io_service;
    boost::asio::ip::tcp::socket socket;
};


struct LibraryLoggingPerformanceTest : public ::testing::Test {
    LibraryLoggingPerformanceTest() {
    }

    virtual ~LibraryLoggingPerformanceTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }

    std::string generateMessage(int size) {
        srand(time(NULL));

        std::ostringstream ss;
        for (int k = 0; k < size; ++k) {
            ss << (char)(rand() % 75 + 48);
        }

        return ss.str();
    }

    void configureTcpOutput(unsigned int totalBytesMessage) {
        // create TcpOutput
        Library::Logging::GenericOutput* output = new Library::Logging::TcpOutput();
        Library::Logging::TcpOutput* tcpOutput = dynamic_cast<Library::Logging::TcpOutput*>(output);

        // Configure TcpOutput
        socket = std::make_shared<ClientSocket>();
        dynamic_cast<ClientSocket*>(socket.get())->setTotalBytesMessage(totalBytesMessage);
        tcpOutput->setSocket(socket);

        tcpOutput->setMaxBufferSize(logBufferSize);

        tcpOutput->setWaitTime(std::chrono::milliseconds(logWaitTimeMS));

        // Inject TcpOutput
        Library::Logging::ServiceLogging::getInstance().setLogOutput(output);

        // Start TcpOutput
        socket->connect(host.c_str(), port.c_str());
        tcpOutput->start();
    }


    template <typename Function>
    void executeFunction(const int threadSize, const int messageTimes, const std::string& messageData, Function&& function) {
        std::vector<std::thread> threads;
        threads.reserve(threadSize);

        elapsedTime.clear();
        elapsedTime.reserve(threadSize);

        for (int index = 0; index < threadSize; ++index) {
            elapsedTime.push_back(0.0);
        }

        for (int index = 0; index < threadSize; ++index) {
            threads.push_back(std::thread(function, index, messageTimes, messageData));
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }

    void calculateStatisticValues(std::vector<double>& values, double& mean, double& deviation) {
        mean = 0.0;
        for (auto& value : values) {
            mean += value;
        }
        mean /= (double)values.size();

        deviation = 0.0;
        for (auto& value : values) {
            deviation += pow(abs(value - mean), 2);
        }
        deviation = sqrt(deviation / (double) (values.size() - 1));
    }

    const int logWaitTimeMS = 1;
    const int logBufferSize = 1500;
    const std::string host {"127.0.0.1"};
    const std::string port {"12345"};

    std::vector<double> elapsedTime;
    std::shared_ptr<Library::Network::GenericSocket> socket;
};


TEST_F(LibraryLoggingPerformanceTest, PerformanceCoutOutput) {
    const int threadSize {20};
    const int messageTimes {1000};
    const int messageSize {99};
    const std::string messageData = generateMessage(messageSize);
    const unsigned int totalBytes = threadSize * messageTimes * (messageSize + 1);


    std::cerr << "Start Standard Out" << std::endl;
    executeFunction(threadSize, messageTimes, messageData,
                    [this](const int id, const int messageTimes, const std::string& messageData) {
                        auto start = std::chrono::high_resolution_clock::now();
                        for (int k = 0; k < messageTimes; ++k) {
                            std::cout << messageData << std::endl;
                        }
                        auto finish = std::chrono::high_resolution_clock::now();
                        elapsedTime[id] = (double)std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
                    });
    std::cerr << "Finish Standard Out" << std::endl;

    double standardMean;
    double standardDeviation;
    calculateStatisticValues(elapsedTime, standardMean, standardDeviation);


    // Configure TcpOutput Test
    Server server(host, port, logBufferSize);
    server.start();
    configureTcpOutput(totalBytes);

    std::cerr << "Start Tcp Output" << std::endl;
    executeFunction(threadSize, messageTimes, messageData,
                    [this](const int id, const int messageTimes, const std::string& messageData) {
                        auto start = std::chrono::high_resolution_clock::now();
                        for (int k = 0; k < messageTimes; ++k) {
                            Library::Logging::Logger().log(messageData);
                        }
                        auto finish = std::chrono::high_resolution_clock::now();
                        elapsedTime[id] = (double)std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
                    });
    std::cerr << "Finish Tcp Output" << std::endl;


    auto* clientSocket = dynamic_cast<ClientSocket*>(socket.get());
    clientSocket->wait();
    clientSocket->close();

    auto totalBytesReceived = server.getBytesReceived();
    auto totalBytesSent = clientSocket->getTotalBytesSent();
    auto totalPacketsSent = clientSocket->getTotalPacketsSent();

    double loggingMean;
    double loggingDeviation;
    calculateStatisticValues(elapsedTime, loggingMean, loggingDeviation);


    //EXPECT_TRUE(totalBytes == totalBytesSent);
    //EXPECT_TRUE(totalBytes == totalBytesReceived);

    std::cerr << "\n\n";
    std::cerr << "Number of Threads: " << threadSize << '\n';
    std::cerr << "Message per Thread: " << messageTimes << '\n';
    std::cerr << "Message Length: " << messageSize  + 1 << " bytes \n";
    std::cerr << "Total Bytes Transferred: " << totalBytes << " bytes \n";
    std::cerr << "Total Packets Transferred: " << totalPacketsSent << " packets \n";
    std::cerr << "Total Packets Size: " << logBufferSize - (logBufferSize % (messageSize  + 1)) << " bytes \n";
    std::cerr << "Logging Wait Time: " << logWaitTimeMS <<  " ms\n";
    std::cerr << "Logging Buffer Size: " << logBufferSize << " bytes \n\n";

    std::cerr.precision(2);
    std::cerr << std::fixed;
    std::cerr << "Standard Output Mean: " << standardMean << " ms\n";
    std::cerr << "Standard Output Deviation: " << standardDeviation << "\n\n";

    std::cerr << "Logging Output Mean: " << loggingMean << " ms\n";
    std::cerr << "Logging Output Deviation: " << loggingDeviation << "\n\n";

    std::cerr << "Total Bytes Sent: " << totalBytesSent << " bytes\n";
    std::cerr << "Total Bytes Received: " << totalBytesReceived << " bytes\n";
    std::cerr << "Standard Output Bandwidth: " << (double)totalBytes / (double)standardMean / 1000.0 << " MBps\n";
    std::cerr << "Logging Output Bandwidth: " << (double)totalBytesReceived / (double)loggingMean / 1000.0 << " MBps\n";
    std::cerr << "Improvement Ratio: " << (double)standardMean / (double)loggingMean << "x\n\n";
}
