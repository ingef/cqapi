// this functions injects the new token in all ConqueryConnection-Objects in the notebook Python scope
const set_token = function () {
    const assign_cmd = `[value.update_token(new_token="${window.keycloak.token}") for name, value in dict(globals()).items() if isinstance(value, ConqueryConnection)]`;
    const kernel = IPython.notebook.kernel;
    kernel.execute(assign_cmd);
    console.log("Token of all ConqueryConnection objects have been updated.")
}


// sets token and then sets timer in refresh_rate seconds for next keycloak token update and new token setting
const update_and_assign_token = function (refresh_rate) {

    set_token()

    // set timer for next update
    setTimeout(function () {
        window.keycloak.updateToken(-1).then(function () {
            update_and_assign_token(refresh_rate)
        }).catch(function () {
            alert('Mit der EVA-Verbindung ist etwas schief gelaufen. Ein erneuter Login ist nötig.');
        });
    }, refresh_rate);
}

// load keycloak module from eva keycloak server
require(["auth_url_placeholder/js/keycloak.js"], function () {

    // initiate keycloak object with server url and client id
    window.keycloak = new Keycloak({
        "realm": "Ingef",
        "auth-server-url": "auth_url_placeholder",
        "ssl-required": "external",
        "resource": "client_id_placeholder",
        "clientId": "client_id_placeholder",
        "public-client": true,
        "confidential-port": 0
    })


    // guide user to keycloak login page
    window.keycloak.init({onLoad: 'login-required'}).then(function (authenticated) {

        // when login was successful we let the user know and set the token in all ConqueryConnections objects
        if (authenticated){
            alert("Authentifizierung war erfolgreich!")
            update_and_assign_token(refresh_rate_placeholder)
        } else {
            alert("Authentifizierung war nicht erfolgreich, bitte wenden Sie sich an das EVA-Team")
        }

    }).catch(function () {
        alert("Initialisierung der Authentifizierung war nicht erfolgreich, bitte wenden Sie sich an das EVA-Team");
    });
})
