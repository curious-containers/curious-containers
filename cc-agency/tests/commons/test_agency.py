from cc_agency.commons.helper import decode_authentication_cookie, encode_authentication_cookie

def test_encode_and_decode_authentication_cookie():
    """
    This function tests the encode_authentication_cookie() and decode_authentication_cookie() 
    functions from cc_agency.commons.helper.
    It checks whether the encoded data returned by encode_authentication_cookie() function matches 
    the expected
    encoded data and whether the decoded data returned by decode_authentication_cookie() function 
    matches the
    original data provided.
    """
    data = ("root","token") 
    encoded_data = "cm9vdA==:token" 
    decoded_data = data
    assert encode_authentication_cookie("root","token") == encoded_data
    assert decode_authentication_cookie(encoded_data) == decoded_data
