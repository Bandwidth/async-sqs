package com.bandwidth.sqs.action.adapter;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.transform.StaxUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;

public class SqsAwsSdkBatchAction<RequestT extends AmazonWebServiceRequest, ResponseT>
extends SqsAwsSdkAction<RequestT, ResponseT>{

    public SqsAwsSdkBatchAction(RequestT request, String requestUrl,
            Marshaller<Request<RequestT>, RequestT> marshaller,
            Unmarshaller<ResponseT, StaxUnmarshallerContext> unmarshaller) {
        super(request, requestUrl, marshaller, unmarshaller);
    }

    @Override
    public boolean isBatchAction(){
        return true;
    }
}
