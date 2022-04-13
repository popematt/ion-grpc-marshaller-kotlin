package com.amazon.ion.grpc

import com.amazon.ion.IonValue
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.MethodDescriptor
import io.grpc.stub.ClientCalls

val SERVICE_NAME = "BeerService"
private val ionObjectMarshaller = IonObjectMarshaller()
private val ionValueMarshaller = IonMarshaller()

val GET_BEER_OBJECT: MethodDescriptor<GetBeerRequest, GetBeerResponse> by lazy {
    val requestMarshaller = ionObjectMarshaller.marshallerFor(GetBeerRequest::class.java)
    val responseMarshaller = ionObjectMarshaller.marshallerFor(GetBeerResponse::class.java)

    MethodDescriptor.newBuilder<GetBeerRequest, GetBeerResponse>(requestMarshaller, responseMarshaller)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "GetBeerObject"))
        .setType(MethodDescriptor.MethodType.UNARY)
        .build()
}

val GET_BEER_ION: MethodDescriptor<IonValue, IonValue> by lazy {
    MethodDescriptor.newBuilder<IonValue, IonValue>(ionValueMarshaller, ionValueMarshaller)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "GetBeerIon"))
        .setType(MethodDescriptor.MethodType.UNARY)
        .build()
}

val GET_BEER_MIXED: MethodDescriptor<IonValue, GetBeerResponse> by lazy {
    val responseMarshaller = ionObjectMarshaller.marshallerFor(GetBeerResponse::class.java)
    MethodDescriptor.newBuilder<IonValue, GetBeerResponse>(ionValueMarshaller, responseMarshaller)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "GetBeerMixed"))
        .setType(MethodDescriptor.MethodType.UNARY)
        .build()
}


class BeerClient(private val channel: Channel) {

    fun listBeers(request: GetBeerRequest): GetBeerResponse = GET_BEER_OBJECT.callBlocking(request)

    fun getIonBeer(request: IonValue): IonValue = GET_BEER_ION.callBlocking(request)

    fun getMixedBeer(request: IonValue): GetBeerResponse = GET_BEER_MIXED.callBlocking(request)

    private fun <ReqT, ResT> MethodDescriptor<ReqT, ResT>.callBlocking(request: ReqT): ResT {
        val call = channel.newCall(this, CallOptions.DEFAULT)
        return ClientCalls.blockingUnaryCall(call, request)
    }
}
