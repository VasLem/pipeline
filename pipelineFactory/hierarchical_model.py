from typing import Union, List, Optional, TypeVar, Generic

from typing import OrderedDict as OrderedDictType

from collections import OrderedDict
from typing import (
    List,
    Tuple,
    Union,
    Generic,
    Optional,
    TypeVar,
)
import networkx as nx
from pygraphviz import AGraph
from networkx import DiGraph

Parent = TypeVar("Parent", bound="HierarchyNode", covariant=True)
HierarchyElement = TypeVar(
    "HierarchyElement", bound=Union["HierarchyLeaf", "HierarchyNode"]
)

Child = TypeVar("Child", bound="Union[HierarchyNode, HierarchyLeaf]", covariant=True)
Leaf = TypeVar("Leaf", bound="HierarchyLeaf", covariant=True)
Node = TypeVar("Node", bound="HierarchyNode", covariant=True)
Self = TypeVar("Self", bound="HierarchyNode")


class HierarchyLeaf(Generic[Node, Leaf, HierarchyElement]):
    def __init__(
        self,
        name: str,
        parent: Optional[Node] = None,
        description: Optional[str] = None,
        hideInShortenedGraph: bool = False,
    ):
        """Hierarchy leaf is a class that represents
        the node of an acyclic graph that does not have any children

        Args:
            name (str): The name of the leaf.
            parent (Optional[Parent], optional): The parent of the leaf. Defaults to None.
            description (Optional[str], optional):  The description of the leaf. Defaults to None.
            hideInShortenedGraph (bool, optional): Whether to hide this leaf when constructing the shortened graph. Defaults to False.
        """
        self.__parent: Optional[Node] = None
        self.name = name
        self.parent = parent
        self.description = description
        self.hideInShortenedGraph = hideInShortenedGraph

    @property
    def parent(
        self,
    ) -> Optional[Node]:
        """
        The parent of self
        """
        return self.__parent

    @parent.setter
    def parent(
        self,
        val: Optional[Node],
    ):
        self.__parent = val

    @property
    def isRoot(self):
        """Returns True if the object has no parent"""
        return self.parent is None

    @property
    def compositeName(self) -> str:
        """

        Returns:
            str: The composite name of the child, which is made by joining its ancestors and its name with a '.'
        """
        return (
            self.parent.compositeName + "." + self.name
            if self.parent is not None
            else self.name
        )

    @property
    def previousCollapsed(self) -> Optional[Leaf]:
        """
        Returns:
            The previous child to this one, ignoring hierarchies
        """
        ancestors = self.ancestors
        if not ancestors:
            return None
        children = ancestors[-1].collapsedChildren
        keys = list(children.keys())
        prevIndex = keys.index(self.compositeName) - 1
        if prevIndex == -1:
            return None
        return children[keys[prevIndex]]

    @property
    def nextCollapsed(self) -> Optional[Leaf]:
        """
        Returns:
            The next child to this one, ignoring hierarchies
        """
        ancestors = self.ancestors
        if not ancestors:
            return None
        children = ancestors[-1].collapsedChildren
        keys = list(children.keys())
        nextIndex = keys.index(self.compositeName) + 1
        if nextIndex == len(children):
            return None
        return children[keys[nextIndex]]

    @property
    def previous(self) -> Optional[HierarchyElement]:
        """
        Returns:
            The previous child to this one
        """
        if self.parent is None:
            return None
        children = self.parent.namedChildren
        keys = list(children.keys())
        prevIndex = keys.index(self.name) - 1
        if prevIndex == -1:
            return None
        return children[keys[prevIndex]]

    @property
    def next(self) -> Optional[HierarchyElement]:
        """
        Returns:
            The next child to this one
        """
        if self.parent is None:
            return None
        children = self.parent.namedChildren
        keys = list(children.keys())
        nextIndex = keys.index(self.name) + 1
        if nextIndex == len(children):
            return None
        return children[keys[nextIndex]]

    def isChildOf(self, parentCandidate: Union[str, Parent]):
        """
        Checks if self is child of a given candidate.

        Args:
            self: Bind the method to an object
            parentCandidate: the candidate

        Returns:
            A boolean value
        """

        if not isinstance(parentCandidate, str):
            parentCandidate = parentCandidate.compositeName
        return tuple(parentCandidate.split(".")) == tuple(
            self.compositeName.split(".")[: len(parentCandidate.split("."))]
        )

    @property
    def ancestors(self) -> List[Node]:
        """
        The ancestors function returns a list of all the parents of self,
            including the parent's parent, and so on.

        Returns:
            A list of all the ancestors of self
        """
        if self.parent is None:
            return []
        return [self.parent] + self.parent.ancestors

    def mostRecentCommonAncestor(
        self, other: Union[str, "HierarchyLeaf"]
    ) -> Optional["HierarchyNode"]:
        """
        The mostRecentCommonAncestor function returns the most recent common ancestor of two nodes.

        Args:
            other: Union[str,  &quot;HierarchyLeaf&quot;]: The other node

        Returns:
            The most recent common ancestor of two nodes
        """

        if not isinstance(other, str):
            other = other.compositeName
        compositeName = self.compositeName
        eq = [
            s != c
            for s, c in zip(compositeName.split(".")[::-1], other.split(".")[::-1])
        ]
        lcaIndex = eq.index(False)
        if lcaIndex == 0:
            return None
        mrcdIndex = len(min(compositeName.split("."), other.split("."))) - lcaIndex
        parent = self.parent
        for _ in range(1, mrcdIndex):
            parent = parent.parent  # type: ignore
        return parent

    def makeGraph(
        self, _currGraph: Optional[DiGraph] = None, nodeCnt: Optional[int] = None
    ) -> Tuple[DiGraph, Optional[int]]:
        """Make the graph of the structure"""
        if _currGraph is None:
            _currGraph = DiGraph()
        _currGraph.add_node(self.compositeName)
        name = self.name
        if nodeCnt is not None:
            name = f"{nodeCnt}.{name}"
        nx.set_node_attributes(  # type: ignore
            _currGraph, {self.compositeName: name}, name="name"
        )
        nx.set_node_attributes(  # type: ignore
            _currGraph, {self.compositeName: "lightblue"}, name="color"
        )
        nx.set_node_attributes(  # type: ignore
            _currGraph,
            {self.compositeName: self.hideInShortenedGraph},
            name="hiddenInShortened",
        )
        nx.set_node_attributes(  # type: ignore
            _currGraph, {self.compositeName: self.description}, name="description"
        )
        if nodeCnt is not None:
            nodeCnt += 1

        return _currGraph, nodeCnt

    def graphToDot(self, graph: DiGraph) -> AGraph:
        """
        The graphToDot function takes a networkx DiGraph and returns an AGraph.
        The function is used to convert the graph into a format that can be rendered by Graphviz.

        Args:
            graph: DiGraph: The directed networkx digraph

        Returns:
            A agraph object
        """

        colors = nx.get_node_attributes(graph, "color")  # type: ignore
        labels = nx.get_node_attributes(graph, "name")  # type: ignore
        edgesLabels = nx.get_edge_attributes(graph, "label")  # type: ignore
        ranks = nx.get_node_attributes(graph, "rank")  # type: ignore
        descriptions = nx.get_node_attributes(graph, "description")  # type: ignore
        a = nx.nx_agraph.to_agraph(graph)
        a.graph_attr["splines"] = "ortho"
        a.graph_attr["overlap"] = "false"
        a.graph_attr["ranksep"] = ".1"
        a.graph_attr["rankdir"] = "tb"

        for i, node in enumerate(a.nodes()):
            n = a.get_node(node)
            n.attr["fillcolor"] = colors[node]
            n.attr["style"] = "filled"
            n.attr["fontcolor"] = "black"
            n.attr["color"] = "black"
            n.attr["fontname"] = "verdana bold"
            n.attr["fontsize"] = "14"
            n.attr["label"] = labels[node]
            n.attr["shape"] = "box"
            if descriptions[node]:
                n.attr["tooltip"] = descriptions[node]
            try:
                del n.attr["rank"]
            except KeyError:
                pass
        if edgesLabels:
            for edge in a.edges():
                if edge not in edgesLabels:
                    continue
                e = a.get_edge(*edge)
                e.attr["label"] = edgesLabels[edge]  # type: ignore
        for sameNodeHeight in [
            [x for x in ranks if ranks[x] == r] for r in set(ranks.values())
        ]:
            if self.name in sameNodeHeight:
                a.add_subgraph(sameNodeHeight, rank="same")
            else:
                a.add_subgraph(sameNodeHeight, rank="same")
        return a
        # print(a.string())


class HierarchyNode(HierarchyLeaf, Generic[Self, Node, Leaf, HierarchyElement]):
    def __init__(
        self: Self,
        name: str,
        children: List[HierarchyElement],
        parent: Optional[Node] = None,
        description: Optional[str] = None,
        hideInShortenedGraph: bool = False,
    ):
        """The Hierachy Node is a node of an acyclig graph with children other nodes or leaves.

        Args:
            name (str): the name of the node
            children (List[Child]): the children of the node
            parent (Optional[Parent], optional): the parent of the node. Defaults to None.
            hideInShortenedGraph (bool, optional): whether to hide this node and its children when making the shortened graph. Defaults to False.
        """
        HierarchyLeaf.__init__(
            self,
            name=name,
            parent=parent,
            hideInShortenedGraph=hideInShortenedGraph,
            description=description,
        )
        self.names = [b.name for b in children]
        self.children = children
        assert len(set(self.names)) == len(
            children
        ), f"Each child of self must have a different name. Supplied: {self.names}"

    @property
    def namedChildren(self) -> OrderedDictType[str, HierarchyElement]:
        """The named children of self

        Returns:
            Dict[str, Union[Block, "Pipeline"]]: named children dict
        """
        return OrderedDict((n, s) for n, s in zip(self.names, self.children))

    def isParentOf(self, childCandidate: Union[str, "HierarchyLeaf"]) -> bool:
        """
        The isParentOf function is used to determine if self
        is the parent of a candidate.
        Args:
            self: Bind the method to a class instance
            childCandidate: the candidate
        Returns:
            A boolean value of true or false
        """

        if not isinstance(childCandidate, str):
            childCandidate = childCandidate.compositeName
        return tuple(self.compositeName.split(".")) == tuple(
            childCandidate.split(".")[: len(self.compositeName.split("."))]
        )

    @property
    def collapsedChildrenAndParents(
        self,
    ) -> OrderedDictType[str, HierarchyElement]:
        """The collapsed named children of self.

        Returns:
            Dict[str, Union[Block, "Pipeline"]]: named children dict
        """
        ret = OrderedDict()
        for child in self.children:
            ret[child.compositeName] = child
            if isinstance(child, HierarchyNode):
                ret.update(child.collapsedChildrenAndParents)

        return ret

    @property
    def collapsedChildren(
        self,
    ) -> OrderedDictType[str, Leaf]:
        """The collapsed named children of self. All parental members are ignored.

        Returns:
            Dict[str, Union[Block, "Pipeline"]]: named children dict
        """
        ret = OrderedDict()
        for child in self.children:
            if isinstance(child, HierarchyNode):
                ret.update(child.collapsedChildren)
            else:
                ret[child.compositeName] = child

        return ret

    @property
    def collapsedParents(
        self,
    ) -> OrderedDictType[str, Node]:
        """The collapsed named parents belonging to self. All leaves are ignored.

        Returns:
            Dict[str, Union[Block, "Pipeline"]]: named children dict
        """
        ret = OrderedDict()
        for child in self.children:
            if isinstance(child, HierarchyNode):
                ret[child.compositeName] = child
                ret.update(child.collapsedParents)

        return ret

    @property
    def firstChild(self) -> Leaf:
        """
        Returns:
            the first leaf of a node
        """
        return list(self.collapsedChildren.values())[0]

    @property
    def lastChild(self) -> Leaf:
        """
        Returns:
            the last leaf of a node
        """
        return list(self.collapsedChildren.values())[-1]

    @property
    def previousCollapsed(self) -> Optional[Leaf]:
        """
        Returns:
            The previous child to this one, ignoring hierarchies
        """
        ancestors = self.ancestors
        if not ancestors:
            return None
        children = ancestors[-1].collapsedChildren
        keys = list(children.keys())
        prevIndex = keys.index(self.firstChild.compositeName) - 1
        if prevIndex == -1:
            return None
        return children[keys[prevIndex]]

    @property
    def nextCollapsed(self) -> Optional[Leaf]:
        """

        Returns:
            The next child to this one, ignoring hierarchies
        """
        ancestors = self.ancestors
        if not ancestors:
            return None
        children = ancestors[-1].collapsedChildren
        keys = list(children.keys())
        nextIndex = keys.index(self.lastChild.compositeName) + 1
        if nextIndex == len(children):
            return None
        return children[keys[nextIndex]]

    def find(self, name: str) -> Child:
        for stepName, step in self.collapsedChildrenAndParents.items():
            if stepName.endswith(name):
                return step
        else:
            raise ValueError(f"Child {name} not found in {self.name}")

    def insertBefore(self: Self, before: str, child: HierarchyElement) -> Self:
        """In place insertion of the children list, before the denoted name.
        Does not support the supply of a composite name.

        Args:
            before: the name of the child, before which the supplied child is added, the composite name can also be provided.
            child: the child to add to self

        Returns:
            self
        """
        children = self.collapsedChildren
        parents = self.collapsedParents
        try:
            name = [name for name in children if name.endswith(before)][0]
        except IndexError:
            raise IndexError(
                f"Provided name {before} not found in the list of children {[x for x in self.children]}"
            )
        prefix = name.replace(before.split(".")[-1], "")
        if prefix != self.compositeName + ".":
            childParent = parents[prefix[:-1]]
            childParent.insertBefore(before.split(".")[-1], child)
            return self
        index = [cnt for cnt, name in enumerate(self.names) if name == before][0]
        self.children.insert(index, child)
        self.names = [b.name for b in self.children]
        return self

    def insertAfter(self: Self, after: str, child: HierarchyElement) -> Self:
        """In place insertion of the children list, after the denoted name.
        Does not support the supply of a composite name.

        Args:
            after: the name of the child, after which the supplied child is added, the composite name can also be provided.
            child: the childe to add to the node

        Returns:
            The node itself
        """
        children = self.collapsedChildren
        parents = self.collapsedParents
        try:
            name = [name for name in children if name.endswith(after)][0]
        except IndexError:
            raise IndexError(
                f"Provided name {after} not found in the list of children {[x for x in self.children]}"
            )
        prefix = name.replace(after.split(".")[-1], "")
        if prefix != self.compositeName + ".":
            childParent = parents[prefix[:-1]]
            childParent.insertAfter(after.split(".")[-1], child)
            return self
        index = [cnt for cnt, name in enumerate(self.names) if name == after][
            0
        ] + 1  # Notice +1
        self.children.insert(index, child)
        self.names = [b.name for b in self.children]
        return self

    def remove(self, childName: str, okNotExist: bool = False):
        """In place Removal of the provided child. It can also not exist.

        Args:
            childName (str): the child name
            okNotExist (bool, optional): Whether to ignore the fact that the child does not exist. Defaults to False.

        Raises:
            IndexError: If the child does not exist and `okNotExist` is False.

        Returns: self.

        """
        children = self.collapsedChildren
        parents = self.collapsedParents
        try:
            name = [
                name for name in self.collapsedChildren if name.endswith(childName)
            ][0]
        except IndexError:
            if okNotExist:
                return self
            raise IndexError(
                f"Provided name {childName} not found in the list of children {[x for x in self.children]}"
            )
        prefix = name.replace(childName.split(".")[-1], "")
        if prefix != self.compositeName + ".":
            childParent = parents[prefix[:-1]]
            childParent.remove(childName.split(".")[-1])
            return self
        index = [cnt for cnt, name in enumerate(self.names) if name == childName][0]
        self.children.pop(index)
        self.names = [b.name for b in self.children]
        return self

    def replace(self: Self, childName: str, newStep: HierarchyElement) -> Self:
        """In place replace of the provided child from self. It can also not exist. The child name has to be
        direct child of self.

        Args:
            childName (str): the child name
            newStep (Block): the new child to replace

        Raises:
            IndexError: If the child does not exist.

        Returns: self.

        """
        children = self.collapsedChildren
        parents = self.collapsedParents
        try:
            name = [name for name in children if name.endswith(childName)][0]
        except IndexError:
            raise IndexError(
                f"Provided name {childName} not found in the list of children {[x for x in self.children]}"
            )
        prefix = name.replace(childName.split(".")[-1], "")
        if prefix != self.compositeName + ".":
            try:
                childParent = parents[prefix[:-1]]
            except:
                raise
            childParent.replace(childName.split(".")[-1], newStep)
            return self
        index = [cnt for cnt, name in enumerate(self.names) if name == childName][0]
        self.children[index] = newStep
        return self

    def append(
        self: Self, children: Union[HierarchyElement, List[HierarchyElement]]
    ) -> Self:
        """In place extension of the children list. Returns self

        Args:
            children: the children to add to self

        Returns:
            self
        """
        if isinstance(children, HierarchyLeaf):
            children = [children]
        self.children.extend(children)
        self.names = [b.name for b in self.children]
        return self

    def prepend(self: Self, children: List[HierarchyElement]) -> Self:
        """In place prepending of the children list. Returns self

        Args:
            children: the children to add to self

        Returns:
            self
        """
        self.children = children + self.children
        self.names = [b.name for b in self.children]
        return self

    def makeGraph(
        self, _currGraph=None, nodeCnt: Optional[int] = 1, shortened: bool = False
    ) -> Tuple[Optional[DiGraph], Optional[int]]:
        """Make the graph of the structure. No arguments are meant to be provided."""
        if self.hideInShortenedGraph and shortened:
            return _currGraph, nodeCnt
        _currGraph, nodeCnt = super().makeGraph(_currGraph, nodeCnt=nodeCnt)
        nx.set_node_attributes(_currGraph, {self.compositeName: "salmon"}, name="color")  # type: ignore
        nx.set_node_attributes(  # type: ignore
            _currGraph, {self.compositeName: self.compositeName}, name="rank"
        )
        previous = self
        l = 0
        while True:
            while (
                shortened
                and (l < len(self.children))
                and self.children[l].hideInShortenedGraph
            ):
                l += 1
            if l == len(self.children):
                break
            nex = self.children[l]
            if isinstance(nex, HierarchyNode):
                _, nodeCnt = nex.makeGraph(
                    _currGraph, nodeCnt=nodeCnt, shortened=shortened
                )
            else:
                _, nodeCnt = nex.makeGraph(_currGraph, nodeCnt=nodeCnt)
            _currGraph.add_edge(previous.compositeName, nex.compositeName)
            nx.set_node_attributes(  # type: ignore
                _currGraph, {nex.compositeName: self.compositeName}, name="rank"
            )
            previous = nex
            l += 1
        return _currGraph, nodeCnt
