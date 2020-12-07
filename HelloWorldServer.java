/*
 * Copyright 2015 The gRPC Authors
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
 */

package io.grpc.examples.helloworld;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.*;
import io.grpc.examples.helloworld.Tripla;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {

  static HashMap<Long, Tripla> banco = new HashMap<Long, Tripla>();
  
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = ServerBuilder.forPort(port)
		.addService(new CreaterImpl())
        .addService(new GreeterImpl())
		.addService(new ReaderImpl())
	.addService(new AtualizarImpl())
	.addService(new DeletarImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          HelloWorldServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final HelloWorldServer server = new HelloWorldServer();
    server.start();
    server.blockUntilShutdown();
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
  static class CreaterImpl extends CreaterGrpc.CreaterImplBase {

    @Override
    public void sayCreate(CreateRequest req, StreamObserver<CreateReply> responseObserver) {
	  
      Tripla tripla = new Tripla();
      //checar chave no hashmap
      if (banco.get(req.getChave()) == null) {
	   //insere e sucesso
	   tripla.versao = 1;
	   tripla.timeStamp = req.getTimeStamp();
	   tripla.dados = req.getDados();
       banco.put(req.getChave(), tripla);
	   CreateReply sucesso = CreateReply.newBuilder().setMessage("Sucesso na insercao!").build();
      responseObserver.onNext(sucesso);
      responseObserver.onCompleted();
	     
      }
      else {
		//retorna erro e (ver,ts,data)
        CreateReply erro = CreateReply.newBuilder().setMessage("Erro: chave ja existente " +" versao: "+banco.get(req.getChave()).versao+" timestamp: "+banco.get(req.getChave()).timeStamp+" dados: "+banco.get(req.getChave()).dados).build();
      responseObserver.onNext(erro);
      responseObserver.onCompleted();
	  }
    }
  }

  static class ReaderImpl extends ReaderGrpc.ReaderImplBase {

    @Override
    public void sayRead(ReadRequest req, StreamObserver<ReadReply> responseObserver) {
	  Tripla tripla = new Tripla();
	  if (banco.get(req.getChave()) == null) {
      		ReadReply erro = ReadReply.newBuilder().setMessage("Erro: chave nao existe ").build();
      responseObserver.onNext(erro);
      responseObserver.onCompleted();
      }
	  else {
	  		ReadReply sucesso = ReadReply.newBuilder().setMessage("Sucesso: "+"versao: "+banco.get(req.getChave()).versao+" timestamp: "+banco.get(req.getChave()).timeStamp+" dados: "+banco.get(req.getChave()).dados).build();
      responseObserver.onNext(sucesso);
      responseObserver.onCompleted();
      }
  }
   
 }
  static class AtualizarImpl extends AtualizarGrpc.AtualizarImplBase {

    @Override
    public void sayAtualiza(AtualizaRequest req, StreamObserver<AtualizaReply> responseObserver) {
	  Tripla tripla = new Tripla();
	  if (banco.get(req.getChave()) == null ) {
	  	AtualizaReply erro = AtualizaReply.newBuilder().setMessage("Erro: chave nao existe!").build();
                responseObserver.onNext(erro);
                responseObserver.onCompleted();		
	  }
	  else {
	  	if (banco.get(req.getChave()).versao == req.getVersao()) {
			tripla.versao = banco.get(req.getChave()).versao + 1;
			tripla.timeStamp = req.getTimeStamp();
			tripla.dados = req.getDados();
			banco.put(req.getChave(),tripla);
			AtualizaReply sucesso = AtualizaReply.newBuilder().setMessage("Sucesso: dados atualizados").build();	
                	responseObserver.onNext(sucesso);
                	responseObserver.onCompleted();	
		}
		else {
			AtualizaReply erro2 = AtualizaReply.newBuilder().setMessage("Erro: versao incompativel").build();
                	responseObserver.onNext(erro2);
                	responseObserver.onCompleted();		
		}
	  }
  }
   
 }
  static class DeletarImpl extends DeletarGrpc.DeletarImplBase {
    
    @Override
    public void sayDeleta(DeletaRequest req, StreamObserver<DeletaReply> responseObserver){
 	Tripla tripla = new Tripla();
	  if (banco.get(req.getChave()) == null ) {
	  	DeletaReply erro = DeletaReply.newBuilder().setMessage("Erro: chave nao existe!").build();
                responseObserver.onNext(erro);
                responseObserver.onCompleted();		
	  }
	  else {
		banco.remove(req.getChave());
		DeletaReply sucesso = DeletaReply.newBuilder().setMessage("Sucesso: entrada removida").build();
                responseObserver.onNext(sucesso);
                responseObserver.onCompleted();	
    	  }   
    }
  }
}
