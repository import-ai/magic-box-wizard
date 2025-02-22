from typing import List, AsyncIterator, Tuple

from common.trace_info import TraceInfo
from wizard.config import Config
from wizard.grimoire.entity.api import ChatRequest, ChatBaseResponse, ChatDeltaResponse, ChatCitationListResponse
from wizard.grimoire.entity.chunk import TextRetrieval, Chunk
from wizard.grimoire.entity.retrieval import Score
from wizard.grimoire.rag import RAG
from wizard.grimoire.retriever.vector_db import AsyncVectorDB


class Pipeline:

    def __init__(self, config: Config):
        self.vector_db: AsyncVectorDB = AsyncVectorDB(config.vector)
        self.max_recall_results: int = config.vector.max_results
        self.rag: RAG = RAG(config.grimoire.openai)

    async def retrieve(self, request: ChatRequest, trace_info: TraceInfo) -> List[TextRetrieval]:
        recall_result_list: List[Tuple[Chunk, float]] = await self.vector_db.query(
            query=request.query, k=self.max_recall_results, condition=request, trace_info=trace_info)
        retrieval_list: List[TextRetrieval] = [
            TextRetrieval(chunk=chunk, score=Score(recall=score, rerank=0))
            for chunk, score in recall_result_list
        ]
        return retrieval_list

    async def astream(self, trace_info: TraceInfo, request: ChatRequest) -> AsyncIterator[ChatBaseResponse]:
        retrieval_list = await self.retrieve(request, trace_info)
        trace_info.info({"retrieval_count": len(retrieval_list)})
        yield ChatCitationListResponse(citation_list=[r.to_citation() for r in retrieval_list])
        async for delta in self.rag.astream(request.query, retrieval_list):
            yield ChatDeltaResponse(delta=delta)
