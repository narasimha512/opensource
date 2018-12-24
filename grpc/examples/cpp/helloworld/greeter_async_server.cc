/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <memory>
#include <iostream>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "helloworld.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::GoodbyeRequest;
using helloworld::GoodbyeReply;
using helloworld::Greeter;

// Class encompasing the state and logic needed to serve a request.
class CallDataBase {
 public:
  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataBase(Greeter::AsyncService* service, ServerCompletionQueue* cq)
      : service_(service), cq_(cq), status_(PROCESS) {}
  virtual ~CallDataBase() {}

  void Proceed() {
    if (status_ == PROCESS) {
      // Spawn a new CallData instance to serve new clients while we process
      // the one for this CallData. The instance will deallocate itself as
      // part of its FINISH state.
      status_ = FINISH;
      Process();
    } else {
      GPR_ASSERT(status_ == FINISH);
      // Once in the FINISH state, deallocate ourselves (CallData).
      delete this;
    }
  }

 protected:
  virtual void Process() = 0;
  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  Greeter::AsyncService* service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue* cq_;

 private:
  // Let's implement a tiny state machine with the following states.
  enum CallStatus { PROCESS, FINISH };
  CallStatus status_;  // The current serving state.
};

class HelloCallData : public CallDataBase {
 public:
  HelloCallData(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataBase(service, cq), responder_(&ctx_) {
    service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_, this);
  }
  
  void Process() override {
    new HelloCallData(service_, cq_);
    // The actual processing.
    std::string prefix("Hello ");
    reply_.set_message(prefix + request_.name());
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    responder_.Finish(reply_, Status::OK, this);
  }

 private:
  ServerContext ctx_;
  HelloRequest request_;
  HelloReply reply_;
  ServerAsyncResponseWriter<helloworld::HelloReply> responder_;  
};

class ByeCallData : public CallDataBase {
  // The same as HelloCallData but with SayBye types and logic.
 public:
  ByeCallData(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataBase(service, cq), responder_(&ctx_) {
    service->RequestSayGoodbye(&ctx_, &request_, &responder_, cq, cq, this);
  }

  void Process() override {
    new ByeCallData(service_, cq_);
    // The actual processing.
    std::string prefix("Goodbye ");
    reply_.set_message(prefix + request_.name());
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    responder_.Finish(reply_, Status::OK, this);
  }

 private:
  ServerContext ctx_;
  GoodbyeRequest request_;
  GoodbyeReply reply_;
  ServerAsyncResponseWriter<helloworld::GoodbyeReply> responder_;
};

class ServerImpl final {
 public:
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    HandleRpcs();
  }

 private:
  // This can be run in multiple threads if needed.
  void HandleRpcs() {
    new HelloCallData(&service_, cq_.get());
    new ByeCallData(&service_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallDataBase*>(tag)->Proceed();
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
  ServerImpl server;
  server.Run();

  return 0;
}
