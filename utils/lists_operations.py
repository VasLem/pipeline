from typing import TypeVar, List

T = TypeVar("T")


def common_elems_with_repeats(first_list: List[T], second_list: List[T]) -> List[T]:
    """Find common elements existing in the supplied list, respecting repeated occurences

    Args:
        first_list (list): the first list
        second_list (list): the second list

    Returns:
         the common elements
    """
    first_list = sorted(first_list)
    second_list = sorted(second_list)
    marker_first = 0
    marker_second = 0
    common = []
    while marker_first < len(first_list) and marker_second < len(second_list):
        if first_list[marker_first] == second_list[marker_second]:
            common.append(first_list[marker_first])
            marker_first += 1
            marker_second += 1
        elif first_list[marker_first] > second_list[marker_second]:
            marker_second += 1
        else:
            marker_first += 1
    return common
