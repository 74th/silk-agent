from gke_session_sandbox._sandboxd_proto import process_pb2


def test_sandboxd_process_messages_round_trip_with_protobuf_six():
    request = process_pb2.ExecuteRequest(
        config=process_pb2.ProcessConfig(command=["python3", "-c", "print('ok')"])
    )

    assert process_pb2.ExecuteRequest.FromString(request.SerializeToString()) == request
