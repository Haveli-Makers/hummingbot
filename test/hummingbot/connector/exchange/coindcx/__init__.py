def test_coindcx_module_imports():
    import hummingbot.connector.exchange.coindcx as coindcx

    assert coindcx is not None
    assert hasattr(coindcx, "__file__")
