"""Clean-up shared by the GKE live tests."""

from google.cloud.firestore_v1.base_query import FieldFilter


async def delete_records_of_conversation(database, prefix: str, conversation_key: str) -> int:
    """Delete the command and Sandbox-operation records a test conversation left behind.

    Found by the conversation key (random, owned by one test run), not by command id: a command
    whose start request failed after the service recorded it never gave the test its id. Returns
    how many records were deleted.
    """
    deleted = 0
    for name in ("commands", "sandbox_operations"):
        query = database.collection(f"{prefix}_{name}").where(
            filter=FieldFilter("conversation_key", "==", conversation_key)
        )
        async for doc in query.stream():
            await doc.reference.delete()
            deleted += 1
    return deleted
