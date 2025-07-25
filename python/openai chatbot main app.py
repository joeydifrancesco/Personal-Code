from dotenv import load_dotenv
from os import environ, path
import openai
import chainlit as cl
from chainlit import Message, on_chat_start
from langchain.chains import RetrievalQA, ConversationalRetrievalChain
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings, HuggingFaceEmbeddings, LlamaCppEmbeddings
from langchain.llms import OpenAI, SelfHostedHuggingFaceLLM, LlamaCpp
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from langchain.docstore.document import Document
from langchain.chains.retrieval_qa.base import BaseRetrievalQA
from langchain.memory import ConversationBufferMemory
load_dotenv()

SYSTEM_TEMPLATE = """Use the following pieces of context to answer the users question.
If you don't know the answer, just say that you don't know, don't try to make up an answer.
ALWAYS return a "SOURCES" part in your answer.
The "SOURCES" part should be a reference to the source of the document from which you got your answer.

Example of your response should be:

```
The answer is foo
SOURCES: xyz
```

Begin!
----------------
{summaries}"""
messages = [
    SystemMessagePromptTemplate.from_template(SYSTEM_TEMPLATE),
    HumanMessagePromptTemplate.from_template("{question}"),
]
prompt = ChatPromptTemplate.from_messages(messages)
chain_type_kwargs = {"prompt": prompt}


embedding_model_name = environ.get("EMBEDDING_MODEL_NAME", 'all-MiniLM-L6-v2')
embedding_type = environ.get("EMBEDDING_TYPE", 'openai')
model_path = environ.get("MODEL_PATH", "")
model_id = environ.get("MODEL_ID", "gpt2")
openai.api_key = environ.get("OPENAI_API_KEY", "")
show_sources = environ.get("SHOW_SOURCES", 'True').lower() in ('true', '1', 't')
retrieval_type = environ.get("RETRIEVAL_TYPE", "conversational")  # conversational/qa
verbose = environ.get("VERBOSE", 'True').lower() in ('true', '1', 't')

# Helpers
def create_embedding_and_llm(embedding_type:str, model_path:str = "", model_id:str = "", embedding_model_name:str = ""):
    """
    Create embedding and llm
    """
    temperature = 0.0
    embedding = None
    llm = None
    streaming = True
    match embedding_type:
        case "llama":
            llm = LlamaCpp(model_path=model_path, seed=0, n_ctx=2048, max_tokens=512, temperature=0.0, streaming=streaming)
            embedding = LlamaCppEmbeddings(model_path=model_path)
        case "openai":
            llm = OpenAI(temperature=temperature, streaming=streaming)
            embedding = OpenAIEmbeddings()
        case "huggingface":
            # gpu = runhouse.cluster(name="rh-a10x", instance_type="A100:1")
            # llm = SelfHostedHuggingFaceLLM(model_id=model_id, hardware=gpu, model_reqs=["pip:./", "transformers", "torch"])
            llm = OpenAI(temperature=temperature)
            embedding = HuggingFaceEmbeddings(model_name=embedding_model_name)
    return (llm, embedding)

@on_chat_start
async def main():
    ''' Startup '''
    openai.api_key = environ["OPENAI_API_KEY"]
    await cl.Avatar(
        name="EI GPT",
        url="",
    ).send()
    await Message(
        content=f"Ask me anything about Econometrica.", author="EI-GPT"
    ).send()


@cl.langchain_factory(use_async=True)
def load_model():
    """ Load model to ask questions of it """

    (llm, embeddings) = create_embedding_and_llm(
            embedding_type=embedding_type,
            model_path=model_path,
            model_id=model_id,
            embedding_model_name=embedding_model_name)

    root_dir = path.dirname(path.realpath(__file__))
    db_dir = f"{root_dir}/db"

    db = FAISS.load_local(db_dir, embeddings)
    retriever = db.as_retriever()

    output_key = "result"
    memory = ConversationBufferMemory(memory_key="chat_history", input_key="question", output_key=output_key, return_messages=True)
    return_source_documents = show_sources
    if retrieval_type == "conversational":
        return ConversationalRetrievalChain.from_llm(
                llm,
                retriever,
                memory=memory,
                output_key=output_key,
                verbose=verbose,
                return_source_documents=return_source_documents)
    else:
        chain_type_kwargs = {
            "memory": memory,
            "verbose": verbose,
            "output_key": output_key
        }
        return RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=retriever,
            return_source_documents=return_source_documents,
            verbose=verbose,
            output_key=output_key,
            chain_type_kwargs=chain_type_kwargs)

@cl.langchain_postprocess
async def process_response(res:dict) -> None:
    """ Format response """
    answer = res["result"]

    elements:list = []
    if show_sources and res.get("source_documents", None) is not None:
        for source in res["source_documents"]:
            src_str:str = source.metadata.get("source", "/").rsplit('/', 1)[-1]
            final_str:str = f"Page {str(source.page_content)}"
            elements.append(cl.Text(content=final_str, name=src_str, display="inline"))

    if verbose:
        print(res)

    await cl.Message(content=answer, elements=elements, author="EI-GPT").send()
