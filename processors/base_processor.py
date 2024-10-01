from typing import Dict, List, Tuple, Any
from tree_sitter import Parser, Node
from tree_sitter_language_pack import get_parser, get_language
from collections import defaultdict
import json
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_query_map():
    try:
        with open('query_map.json', 'r') as file:
            json_str = file.read()
        
        query_map = json.loads(json_str)
        return query_map
    except IOError as e:
        logging.error(f"Error reading query_map.json: {str(e)}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing query_map.json: {str(e)}")
        return {}

QUERY_MAP = load_query_map()

class BaseProcessor:
    MAX_CHUNK_SIZE: int = 2048
    MIN_CHUNK_SIZE: int = 1024

    def __init__(self, language: str):
        self.parser: Parser = get_parser(language)
        self.type: str = language

    def get_chunks(self, root_node: Node, file_name: str) -> List[Dict[str, Any]]:
        chunks = []
        try:
            source_code = root_node.text.decode('utf-8')
        except UnicodeDecodeError as e:
            logging.error(f"Error decoding source code: {str(e)}")
            return chunks

        def add_chunk(start_byte: int, end_byte: int, start_point: Tuple[int, int], end_point: Tuple[int, int], chunk_type: str) -> None:
            chunk_text = source_code[start_byte:end_byte]
            chunks.append({
                "content": chunk_text,
                "metadata": {
                    "id": len(chunks),
                    "type": chunk_type,
                    "start_point": start_point,
                    "end_point": end_point,
                    "file_name": file_name
                }
            })

        def process_nodes(nodes: List[Node]) -> None:
            current_chunk_nodes = []
            current_chunk_size = 0

            i = 0
            while i < len(nodes):
                node = nodes[i]
                try:
                    node_size = len(node.text.decode('utf-8'))
                except UnicodeDecodeError:
                    logging.warning(f"Error decoding node text, skipping node")
                    i += 1
                    continue

                if current_chunk_size + node_size < self.MIN_CHUNK_SIZE:
                    current_chunk_nodes.append(node)
                    current_chunk_size += node_size
                    i += 1
                    # Keep adding nodes until we reach MIN_CHUNK_SIZE
                    while current_chunk_size < self.MIN_CHUNK_SIZE and i < len(nodes):
                        next_node = nodes[i]
                        try:
                            next_node_size = len(next_node.text.decode('utf-8'))
                        except UnicodeDecodeError:
                            logging.warning(f"Error decoding node text, skipping node")
                            i += 1
                            continue
                        current_chunk_nodes.append(next_node)
                        current_chunk_size += next_node_size
                        i += 1
                    # Create chunk with accumulated nodes
                    if current_chunk_nodes:
                        start_byte = current_chunk_nodes[0].start_byte
                        end_byte = current_chunk_nodes[-1].end_byte
                        start_point = current_chunk_nodes[0].start_point
                        end_point = current_chunk_nodes[-1].end_point
                        add_chunk(start_byte, end_byte, start_point, end_point, "mix")
                    current_chunk_nodes = []
                    current_chunk_size = 0
                elif node_size > self.MAX_CHUNK_SIZE and len(node.children) > 0:
                    # Node is too big, process its children
                    process_nodes(node.children)
                    i += 1
                else:
                    add_chunk(node.start_byte, node.end_byte, node.start_point, node.end_point, node.type)
                    i += 1

        process_nodes(root_node.children)
        return chunks

    def process_code(self, file_name: str = None, source_code: str = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        try:
            tree = self.parser.parse(bytes(source_code, "utf8"))
        except Exception as e:
            logging.error(f"Error parsing source code: {str(e)}")
            return [], {}

        chunks = self.get_chunks(tree.root_node, file_name)
        return chunks
