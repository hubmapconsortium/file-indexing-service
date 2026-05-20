import sys
import os
import json
import logging
import configparser
import requests
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger(__name__)

class FileIndexingUtils:

    portfolio_config_dict = {}
    # If the process code calling this class's constructor indicates
    # Slack notifications should be disabled (e.g. during development),
    # set this global to True
    slack_notification_disabled = False

    def __init__(self, config_file_name:str, disable_slack_notifications=False):
        global slack_notification_disabled

        slack_notification_disabled = disable_slack_notifications

        #
        # Read configuration from the project INI file and set global constants
        #
        config = configparser.ConfigParser()

        if not os.path.exists(config_file_name):
            raise FileNotFoundError(f"Unable to find project initialization file {config_file_name}.")

        config.read(config_file_name)

        # Put the configuration read in the log when debugging
        logger.debug(f"Begin configuration read from config_file_name={config_file_name}:")
        for section in config.sections():
            logger.debug(f"[{section}]")
            for key, val in config.items(section):
                logger.debug(f"{key} = {val}")
        logger.debug(f"End configuration read from the initialization file.")

        self.portfolio_config_dict['SLACK_SUPPORTED_CHANNELS'] = config.get('SlackNotificationSettings', 'SLACK_SUPPORTED_CHANNELS')
        self.portfolio_config_dict['SLACK_CHANNEL_TOKEN'] = config.get('SlackNotificationSettings', 'SLACK_CHANNEL_TOKEN')
        self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH'] = int(config.get('SlackNotificationSettings', 'SLACK_MAX_MSG_LENGTH'))
        self.portfolio_config_dict['SLACK_NOTIFICATION_CHANNEL'] = config.get('SlackNotificationSettings', 'SLACK_NOTIFICATION_CHANNEL')
        self.portfolio_config_dict['SLACK_BAD_NEWS_EMOJI'] = config.get('SlackNotificationSettings', 'SLACK_BAD_NEWS_EMOJI')
        self.portfolio_config_dict['SLACK_GOOD_NEWS_EMOJI'] = config.get('SlackNotificationSettings', 'SLACK_GOOD_NEWS_EMOJI')
        self.portfolio_config_dict['SLACK_NEUTRAL_INFO_EMOJI'] = config.get('SlackNotificationSettings', 'SLACK_NEUTRAL_INFO_EMOJI')
        self.portfolio_config_dict['SLACK_USER_ID_MENTIONS_ON_ERROR'] = config.get('SlackNotificationSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR')
        self.portfolio_config_dict['SLACK_USER_ID_MENTIONS_ON_SUCCESS'] = config.get('SlackNotificationSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS')
        print(f"Project configuration loaded from {config_file_name}.")

    def get_config(self)->dict:
        return self.portfolio_config_dict

    # Get a dict from the JSON which tracks the log files previously processed.
    def get_tracking_from_file(self, filename:str)->dict:
        with open(filename, 'r') as f:
            json_as_dict=json.load(f)
        return json_as_dict

    # Save a dict which tracks the log files previously processed as JSON.
    def overwrite_tracking_to_file(self, filename:str, pydict:dict):
        dict_as_json=json.dumps(obj=pydict
                                , indent=2
                                , sort_keys=True)
        if os.path.isfile(filename):
            logger.info(f"Overwriting file '{filename}'.")
        with open(filename, 'w') as jf:
            jf.write(dict_as_json)
        logger.info(f"Wrote {len(dict_as_json)} bytes of JSON to '{filename}'\n")

    # Given a non-empty list, log the reasons processing is being halted, then exit
    def exit_if_halt_reason(self, halt_reasons=[], slack_channel=None, mentions_dict:dict=None,
                            process_bad_news_emoji=':bangbang:', exit_code=2):
        if not halt_reasons:
            return
        bad_news = f"{process_bad_news_emoji} Processing halted for the following logged reasons:"
        logger.error(f" Processing halted for the following reasons:")
        for idx, halt_reason in enumerate(halt_reasons):
            logger.error(f"\t{halt_reason}")
            bad_news += f"\n{idx+1}) {halt_reason}"
        if slack_channel:
            self.postToSlackChannel(channel = slack_channel
                                    , msg = bad_news
                                    , mentions_dict = mentions_dict)
        sys.exit(exit_code)

    def get_nested_value_recursive(self, obj, dotted_path):
        if not dotted_path:
            return obj
        if not isinstance(obj, dict):
            return None

        if not '.' in dotted_path:
            return obj.get(dotted_path)
    
        head, tail = dotted_path.split('.', 1)
        return self.get_nested_value_recursive(obj.get(head), tail)

    ####################################################################################################
    ## Slack Notification
    ####################################################################################################

    """
    Post a string to target Slack channel

    Input
    --------
    POST request body data is a JSON object containing the following fields:
        message : str
            The message to be sent to the channel. Required.
        channel : str
            The target Slack channel. Optional, with default from configuration used if not specified.
        send_to_email : bool
            Indication if the message should also be sent via email to addresses configured in MAIL_ADMIN_LIST.
            Optional, defaulting to False when not in the JSON.
    Returns
    --------
    dict
        Dictionary with separate dictionary entries for 'Slack' and 'Email', each containing a summary of the notification.
    """
    def postToSlackChannel(self, channel:str, msg:str, mentions_dict:dict=None):
        global slack_notification_disabled
        
        if slack_notification_disabled:
            return
        
        # Not doing user authorization for this utility, which is only for use on internal apps

        if not channel:
            raise Exception('The Slack channel to post a message to must be specified.')
        if channel not in self.portfolio_config_dict['SLACK_SUPPORTED_CHANNELS']:
            raise Exception('A supported Slack channel must be specified to post a message.')

        if not msg:
            raise Exception('The message to post to Slack must be a non-blank string.')
        if len(msg) > self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH']:
            raise Exception(f"The message to post to Slack must be"
                            f" {self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH']}"
                            f" characters or less.")
        # If any mentions are to occur, pull them from mentions_dict for inclusion.
        mention_text = ''
        if mentions_dict:
            mention_text = '\n\nAttn:'
            for dev_name, slack_user_id in mentions_dict.items():
                mention_text += f" <@{slack_user_id}>"
                
        # Send message to Slack
        target_url = 'https://slack.com/api/chat.postMessage'
        request_header = {
            "Authorization": f"Bearer {self.portfolio_config_dict['SLACK_CHANNEL_TOKEN']}"
        }
        json_to_post = {
            "channel": channel
            , "text": msg + mention_text
        }

        logger.debug("======postToSlackChannel() json_to_post======")
        logger.debug(json_to_post)

        response = requests.post(url = target_url, headers = request_header, json = json_to_post, verify = False)

        notification_results = {'Slack': None}
        # Note: Slack API wraps the error response in the 200 response instead of using non-200 status code
        # Callers should always check the value of the 'ok' params in the response
        if response.status_code == 200:
            result = response.json()
            # 'ok' filed is boolean value
            if 'ok' in result:
                if result['ok']:
                    output = {
                        "channel": channel,
                        "message": msg
                    }
    
                    logger.debug("======notify() Sent Notification Summary======")
                    logger.info(output)

                    return output
                else:
                    logger.error(f"Unable to notify Slack channel: {channel} with the msg: {msg}")
                    logger.debug("======notify() response json from Slack API======")
                    logger.debug(result)

                    # https://api.slack.com/methods/chat.postMessage#errors
                    if 'error' in result:
                        raise Exception(result['error'])
                    else:
                        raise Exception("Slack API unable to process the request, 'error' param/field missing from Slack API response json")
            else:
                raise Exception("The 'ok' param/field missing from Slack API response json")
        else:
            raise Exception("Failed to send a request to Slack API")

        return # Shouldn't get here
