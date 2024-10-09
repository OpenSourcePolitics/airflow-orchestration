matomo_requests_config = {
    'hourly_visits': {'method': 'VisitTime.getVisitInformationPerLocalTime', 'flat': 1},
}

matomo_requests_config_all = {
    'visits': {'method': 'API.get', 'format_metrics': 1, 'transpose': True},
    'pages': {'method': 'Actions.getPageUrls', 'flat': 1},
    'page_titles': {'method': 'Actions.getPageTitles', 'flat': 1},
    'referrers': {'method': 'Referrers.getReferrerType', 'format_metrics': 1},
    'user_language': {'method': 'UserLanguage.getLanguage'},
    'downloads': {'method': 'Actions.getDownloads', 'flat': 1},
    'entry_pages': {'method': 'Actions.getEntryPageUrls', 'flat': 1},
    'entry_page_titles': {'method': 'Actions.getEntryPageTitles', 'flat': 1},
    'exit_pages': {'method': 'Actions.getExitPageUrls', 'flat': 1},
    'exit_page_titles': {'method': 'Actions.getExitPageTitles', 'flat': 1},
    'users_country': {'method': 'UserCountry.getCountry', 'format_metrics': 1},
    'users_city': {'method': 'UserCountry.getCity', 'format_metrics': 1},
    'visits_duration': {'method': 'VisitorInterest.getNumberOfVisitsPerPage', 'format_metrics': 1},
    'visits_number_of_page_per_visit': {'method': 'VisitorInterest.getNumberOfVisitsPerPage', 'format_metrics': 1},
    'visits_per_number_of_visits': {'method': 'VisitorInterest.getNumberOfVisitsByVisitCount', 'format_metrics': 1},
    'visits_per_returning_time': {'method': 'VisitorInterest.getNumberOfVisitsByDaysSinceLast', 'format_metrics': 1},
    'device_type': {'method': 'DevicesDetection.getType', 'format_metrics': 1}
}