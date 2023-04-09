from typing import List
def prime_factors(n: int) -> List[int]:
    """Performs prie factorization of a number

    Args:
        n (int): the number

    Returns:
        List[int]: the factors of the number
    """
    i = 2
    factors = []
    while i * i <= n:
        if n % i:
            i += 1
        else:
            n //= i
            factors.append(i)
    if n > 1:
        factors.append(n)
    return factors