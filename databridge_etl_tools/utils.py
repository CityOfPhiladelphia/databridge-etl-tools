import click
import re

def pass_params_to_ctx(context: 'click.Context', **kwargs): 
    '''Take a context and assign kwargs to it to be passed to child commands'''
    for param, value in kwargs.items(): 
        context.obj[param] = value
    return context

def force_2d(wkt: str) -> str:
    """Take out the Z and/or M dimension values from a WKT string if they exist."""
    # TODO: Consider how to return captured Z and M values, to be put on the table as new columns if they are substantively meaningful

    # remove Z, M, or ZM label from shape type
    SHAPE_TYPE_RE = r'(?P<shapetype>\w+)(?P<zm> ZM?| Z| M)?\s*\('
    wkt = re.sub(SHAPE_TYPE_RE, r'\1(', wkt)

    # capture x and y coordinates; also capture z and m coordinates if present (each match is one point within the shape)
    COORDINATE_RE = r'(?P<X>\d+\.?\d*)\s+(?P<Y>\d+\.?\d*)(\s+(?P<Z>\d+\.?\d*|NaN)(\s+(?P<M>\d+\.?\d*|NaN)?)?)?'

    def remove_zm(match: re.Match) -> str:
        """Keep only the x and y. Is applied to every point within a shape."""
        x, y, _, z, _, m = match.groups()
        return f"{x} {y}"
    
    return re.sub(COORDINATE_RE, remove_zm, wkt)